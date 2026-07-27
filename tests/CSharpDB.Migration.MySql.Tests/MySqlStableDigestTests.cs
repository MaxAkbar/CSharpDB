using System.Text;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlStableDigestTests
{
    [Fact]
    public void DigestFramesValuesAndDistinguishesNullFromEmpty()
    {
        string withNull = MySqlStableDigest.Text(
            "domain",
            new string?[] { null });
        string withEmpty = MySqlStableDigest.Text("domain", string.Empty);
        string splitAfterOne = MySqlStableDigest.Text("domain", "a", "bc");
        string splitAfterTwo = MySqlStableDigest.Text("domain", "ab", "c");

        Assert.NotEqual(withNull, withEmpty);
        Assert.NotEqual(splitAfterOne, splitAfterTwo);
        Assert.Equal(
            withNull,
            MySqlStableDigest.Sequence("domain", new string?[] { null }));
        Assert.Equal(
            withEmpty,
            MySqlStableDigest.Sequence("domain", [string.Empty]));
        Assert.Matches("^[0-9a-f]{64}$", withNull);
    }

    [Fact]
    public void DigestRejectsInvalidUnicode()
    {
        Assert.Throws<EncoderFallbackException>(
            () => MySqlStableDigest.Text("domain", "invalid\uD800value"));
    }
}
