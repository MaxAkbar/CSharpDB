using CSharpDB.Migration.SqlServer;
using System.Text;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerStableDigestTests
{
    [Fact]
    public void DigestDistinguishesNullFromEmpty()
    {
        string withNull = SqlServerStableDigest.Text(
            "domain",
            new string?[] { null });
        string withEmpty = SqlServerStableDigest.Text("domain", string.Empty);

        Assert.NotEqual(withNull, withEmpty);
        Assert.Equal(
            withNull,
            SqlServerStableDigest.Sequence("domain", new string?[] { null }));
        Assert.Equal(
            withEmpty,
            SqlServerStableDigest.Sequence("domain", [string.Empty]));
    }

    [Fact]
    public void DigestRejectsInvalidUnicode()
    {
        Assert.Throws<EncoderFallbackException>(
            () => SqlServerStableDigest.Text("domain", "invalid\uD800value"));
    }
}
