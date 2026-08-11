using System.Globalization;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Observability.Tests;

public sealed class QueryFingerprintTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(
        "SELECT id FROM users WHERE name = 'Alice' AND age >= 21",
        " select ID from USERS /* ignored */ where NAME='Bob' and AGE >= 99 ")]
    [InlineData(
        "SELECT * FROM t WHERE id = @first",
        "SELECT * FROM T WHERE ID = @second")]
    [InlineData(
        "SELECT * FROM t WHERE id != 1",
        "SELECT * FROM t WHERE id <> 2")]
    [InlineData(
        "SELECT \"display name\" FROM \"order detail\"",
        "select \"DISPLAY NAME\" from \"ORDER DETAIL\"")]
    public void EquivalentQueryShapes_ProduceTheSameFingerprint(
        string first,
        string second)
    {
        QueryFingerprint firstFingerprint = SqlQueryNormalizer.CreateFingerprint(first, Ct);
        QueryFingerprint secondFingerprint = SqlQueryNormalizer.CreateFingerprint(second, Ct);

        Assert.Equal(firstFingerprint, secondFingerprint);
    }

    [Theory]
    [InlineData("SELECT value FROM observability_baseline WHERE id = 512")]
    [InlineData("INSERT INTO observability_baseline VALUES (1042, 10420, 'sql')")]
    [InlineData("SELECT id, value FROM observability_baseline LIMIT 128")]
    [InlineData("select id from users /* ignored */ where value != @secret and deleted is null;")]
    [InlineData("INSERT INTO blobs VALUES (1, X'DEADBEEF')")]
    public void AllocationLightFingerprintPath_MatchesCanonicalTokenizerContract(
        string sql)
    {
        QueryFingerprint optimized = SqlQueryNormalizer.CreateFingerprint(sql, Ct);
        QueryFingerprint canonical = SqlQueryNormalizer
            .NormalizeAndFingerprint(sql, Ct)
            .Fingerprint;

        Assert.Equal(canonical, optimized);
    }

    [Fact]
    public void NormalizeAndFingerprint_RemovesLiteralAndParameterValuesBeforePublication()
    {
        const string sql = """
            SELECT secret_col
            FROM accounts
            WHERE name = 'Canary-Password-Value'
              AND amount = 123456789
              AND ratio = 3.14159
              AND payload = X'DEADBEEF'
              AND owner_id = @BearerCapabilitySecret
              AND deleted_at IS NULL;
            """;

        QueryFingerprintResult result = SqlQueryNormalizer.NormalizeAndFingerprint(sql, Ct);

        Assert.DoesNotContain("Canary", result.NormalizedText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Password", result.NormalizedText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("123456789", result.NormalizedText, StringComparison.Ordinal);
        Assert.DoesNotContain("3.14159", result.NormalizedText, StringComparison.Ordinal);
        Assert.DoesNotContain("DEADBEEF", result.NormalizedText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("BearerCapabilitySecret", result.NormalizedText, StringComparison.Ordinal);
        Assert.Contains("?", result.NormalizedText, StringComparison.Ordinal);
        Assert.StartsWith(QueryFingerprint.Algorithm + ":", result.Fingerprint.Value, StringComparison.Ordinal);
        Assert.Equal(QueryFingerprint.Algorithm.Length + 1 + 64, result.Fingerprint.Value.Length);
    }

    [Fact]
    public void Fingerprint_DistinguishesStructuralAndIdentifierChanges()
    {
        QueryFingerprint baseline =
            SqlQueryNormalizer.CreateFingerprint("SELECT id FROM users WHERE id = 1", Ct);

        Assert.NotEqual(
            baseline,
            SqlQueryNormalizer.CreateFingerprint("SELECT name FROM users WHERE id = 1", Ct));
        Assert.NotEqual(
            baseline,
            SqlQueryNormalizer.CreateFingerprint("SELECT id FROM orders WHERE id = 1", Ct));
        Assert.NotEqual(
            baseline,
            SqlQueryNormalizer.CreateFingerprint("SELECT id FROM users WHERE id > 1", Ct));
    }

    [Fact]
    public void IdenticalNormalizedText_AlwaysHasTheSameFingerprint()
    {
        QueryFingerprintResult integer =
            SqlQueryNormalizer.NormalizeAndFingerprint("SELECT * FROM t WHERE value = 42", Ct);
        QueryFingerprintResult text =
            SqlQueryNormalizer.NormalizeAndFingerprint("SELECT * FROM t WHERE value = 'secret'", Ct);
        QueryFingerprintResult parameter =
            SqlQueryNormalizer.NormalizeAndFingerprint("SELECT * FROM t WHERE value = @secret", Ct);

        Assert.Equal(integer.NormalizedText, text.NormalizedText);
        Assert.Equal(integer.NormalizedText, parameter.NormalizedText);
        Assert.Equal(integer.Fingerprint, text.Fingerprint);
        Assert.Equal(integer.Fingerprint, parameter.Fingerprint);
    }

    [Fact]
    public void Fingerprint_IsCultureInvariant()
    {
        CultureInfo previousCulture = CultureInfo.CurrentCulture;
        CultureInfo previousUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("tr-TR");
            QueryFingerprint turkish =
                SqlQueryNormalizer.CreateFingerprint("select identity from identities where id = 17", Ct);

            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");
            QueryFingerprint english =
                SqlQueryNormalizer.CreateFingerprint("SELECT IDENTITY FROM IDENTITIES WHERE ID = 42", Ct);

            Assert.Equal(english, turkish);
        }
        finally
        {
            CultureInfo.CurrentCulture = previousCulture;
            CultureInfo.CurrentUICulture = previousUiCulture;
        }
    }

    [Fact]
    public void Fingerprint_UsesAStableVersionedGoldenVector()
    {
        QueryFingerprint fingerprint = SqlQueryNormalizer.CreateFingerprint(
            "SELECT name FROM users WHERE id = 42 AND active != @enabled;",
            Ct);

        Assert.Equal(
            "csharpdb-sql-v1:267cb253d89121ac66ea8ea775a232af77173dfbabea0c43cd9d2b726ac093e5",
            fingerprint.Value);
    }

    [Fact]
    public void OversizedTokenStream_FailsWithAResourceLimitCode()
    {
        string sql = "SELECT " + string.Join(",", Enumerable.Repeat("1", 50_001));

        CSharpDbException exception = Assert.Throws<CSharpDbException>(
            () => SqlQueryNormalizer.CreateFingerprint(sql, Ct));

        Assert.Equal(ErrorCode.ResourceLimitExceeded, exception.Code);
        Assert.DoesNotContain(sql, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OversizedSqlText_FailsBeforeTokenizationWithoutEchoingInput()
    {
        string sql = "SELECT '" + new string('x', 1_048_576) + "'";

        CSharpDbException exception = Assert.Throws<CSharpDbException>(
            () => SqlQueryNormalizer.CreateFingerprint(sql, Ct));

        Assert.Equal(ErrorCode.ResourceLimitExceeded, exception.Code);
        Assert.DoesNotContain(new string('x', 128), exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("sha256:00")]
    [InlineData("csharpdb-sql-v1:ABCDEF0000000000000000000000000000000000000000000000000000")]
    public void QueryFingerprint_RejectsUnknownOrNonCanonicalValues(string value)
    {
        Assert.ThrowsAny<ArgumentException>(() => new QueryFingerprint(value));
    }
}
