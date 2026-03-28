using CSharpDB.Execution;
using CSharpDB.Primitives;
using System.Globalization;

namespace CSharpDB.Tests;

public sealed class CollationSupportTests
{
    [Fact]
    public void NormalizeMetadataName_CanonicalizesRegisteredCollations_AndPreservesUnknownNames()
    {
        Assert.Null(CollationSupport.NormalizeMetadataName(null));
        Assert.Equal("BINARY", CollationSupport.NormalizeMetadataName(" binary "));
        Assert.Equal("NOCASE", CollationSupport.NormalizeMetadataName(" nocase "));
        Assert.Equal("NOCASE_AI", CollationSupport.NormalizeMetadataName(" nocase_ai "));
        Assert.Equal("ICU:tr-TR", CollationSupport.NormalizeMetadataName(" icu:TR-tr "));
        Assert.Equal("CUSTOM_RULES", CollationSupport.NormalizeMetadataName(" custom_rules "));

        Assert.True(CollationSupport.IsSupported("BINARY"));
        Assert.True(CollationSupport.IsSupported("nocase"));
        Assert.True(CollationSupport.IsSupported("NOCASE_AI"));
        Assert.True(CollationSupport.IsSupported("ICU:tr-TR"));
        Assert.False(CollationSupport.IsSupported("custom_rules"));
    }

    [Fact]
    public void SemanticallyEquals_TreatsNullAsBinaryDefault()
    {
        Assert.True(CollationSupport.SemanticallyEquals(null, "BINARY"));
        Assert.True(CollationSupport.SemanticallyEquals(" binary ", null));
        Assert.True(CollationSupport.SemanticallyEquals("NOCASE", "nocase"));
        Assert.True(CollationSupport.SemanticallyEquals("nocase_ai", "NOCASE_AI"));
        Assert.True(CollationSupport.SemanticallyEquals("icu:TR-tr", "ICU:tr-TR"));
        Assert.False(CollationSupport.SemanticallyEquals("NOCASE", "BINARY"));
    }

    [Fact]
    public void RegistryBackedTextHelpers_PreserveBinaryAndNoCaseBehavior()
    {
        Assert.NotEqual(0, CollationSupport.CompareText("Alpha", "alpha", null));
        Assert.Equal(0, CollationSupport.CompareText("Alpha", "alpha", "NOCASE"));

        Assert.Equal("Alpha", CollationSupport.NormalizeText("Alpha", null));
        Assert.Equal("ALPHA", CollationSupport.NormalizeText("Alpha", "NOCASE"));
        Assert.Equal("JOSE", CollationSupport.NormalizeText("José", "NOCASE_AI"));

        DbValue binary = CollationSupport.NormalizeIndexValue(DbValue.FromText("Alpha"), null);
        DbValue noCase = CollationSupport.NormalizeIndexValue(DbValue.FromText("Alpha"), "NOCASE");
        DbValue noCaseAi = CollationSupport.NormalizeIndexValue(DbValue.FromText("José"), "NOCASE_AI");
        Assert.Equal("Alpha", binary.AsText);
        Assert.Equal("ALPHA", noCase.AsText);
        Assert.Equal("JOSE", noCaseAi.AsText);
        Assert.Equal(0, CollationSupport.Compare(DbValue.FromText("Alpha"), DbValue.FromText("alpha"), "NOCASE"));
        Assert.Equal(0, CollationSupport.CompareText("José", "JOSE", "NOCASE_AI"));
    }

    [Fact]
    public void IcuCollation_UsesCanonicalCultureAndSortKeys()
    {
        string canonical = CollationSupport.NormalizeMetadataName("icu:SV-se")!;
        Assert.Equal($"ICU:{CultureInfo.GetCultureInfo("sv-SE").Name}", canonical);

        string left = "ä";
        string right = "z";
        int expected = CompareUsingSortKeys("sv-SE", left, right);
        int actual = Math.Sign(CollationSupport.CompareText(left, right, "ICU:sv-SE"));
        Assert.Equal(expected, actual);
    }

    private static int CompareUsingSortKeys(string locale, string left, string right)
    {
        byte[] leftKey = CultureInfo.GetCultureInfo(locale).CompareInfo.GetSortKey(left, CompareOptions.None).KeyData;
        byte[] rightKey = CultureInfo.GetCultureInfo(locale).CompareInfo.GetSortKey(right, CompareOptions.None).KeyData;
        return Math.Sign(leftKey.AsSpan().SequenceCompareTo(rightKey));
    }
}
