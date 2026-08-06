using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class XmlQueryFunctionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Registry_DescribesXmlFunctionsAndAlias()
    {
        Assert.True(DbBuiltInFunctionRegistry.TryGet("XML_EXISTS", out var exists));
        Assert.True(DbBuiltInFunctionRegistry.TryGet("XMLEXISTS", out var alias));
        Assert.Same(exists, alias);
        Assert.Equal(2, exists.MinimumArity);
        Assert.Equal(3, exists.MaximumArity);
        Assert.Equal(DbType.Integer, exists.ReturnType);
        Assert.Equal("boolean integer", exists.ReturnTypeRule);
        Assert.Equal(DbFunctionNullBehavior.Propagates, exists.NullBehavior);
        Assert.True(exists.IsDeterministic);

        Assert.True(DbBuiltInFunctionRegistry.TryGet("XML_VALUE", out var value));
        Assert.Equal(2, value.MinimumArity);
        Assert.Equal(3, value.MaximumArity);
        Assert.Equal(DbType.Text, value.ReturnType);
        Assert.Equal(DbFunctionNullBehavior.Propagates, value.NullBehavior);

        Assert.Throws<ArgumentException>(() => DbFunctionRegistry.Create(functions =>
            functions.AddScalar("xmlexists", 2, static (_, _) => DbValue.Null)));
    }

    [Fact]
    public async Task XmlFunctions_FilterAndExtractFromStoredXml()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await database.ExecuteAsync(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, payload XML)",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO documents VALUES " +
            "(1, '<order id=\"o-1\"><customer id=\"c-1\">Ada</customer>" +
            "<items><item sku=\"A1\">First</item><item sku=\"B2\">Second</item></items></order>'), " +
            "(2, '<order id=\"o-2\"><customer id=\"c-2\">Grace</customer>" +
            "<items><item sku=\"B2\">Only</item></items></order>')",
            Ct);

        await using QueryResult result = await database.ExecuteAsync(
            "SELECT id, XML_VALUE(payload, '/order/customer/@id') AS customer_id, " +
            "XML_EXISTS(payload, '/order/items/item[@sku=\"A1\"]') AS has_a1 " +
            "FROM documents " +
            "WHERE XMLEXISTS(payload, '/order/items/item[@sku=\"A1\"]') = 1",
            Ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(1, row[0].AsInteger);
        Assert.Equal("c-1", row[1].AsText);
        Assert.Equal(1, row[2].AsInteger);
        Assert.Equal(DbType.Text, result.Schema[1].Type);
        Assert.Equal(SqlTypeKind.Boolean, result.Schema[2].EffectiveType.Kind);
    }

    [Fact]
    public async Task XmlFunctions_SupportNamespaceMapsAndStandardXmlPrefix()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await using QueryResult result = await database.ExecuteAsync(
            """
            SELECT XML_EXISTS(
                       '<order xmlns="urn:orders"><customer id="c-7"/></order>',
                       '/o:order/o:customer[@id="c-7"]',
                       '{"o":"urn:orders"}'),
                   XML_VALUE(
                       '<order xmlns="urn:orders"><customer id="c-7"/></order>',
                       '/o:order/o:customer/@id',
                       '{"o":"urn:orders"}'),
                   XML_VALUE('<root xml:lang="en"/>', '/root/@xml:lang')
            """,
            Ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(1, row[0].AsInteger);
        Assert.Equal("c-7", row[1].AsText);
        Assert.Equal("en", row[2].AsText);
    }

    [Fact]
    public async Task XmlFunctions_PropagateNullsAndUseXPathScalarConversions()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await using QueryResult result = await database.ExecuteAsync(
            """
            SELECT XML_EXISTS(NULL, '/root'),
                   XML_VALUE('<root/>', NULL),
                   XML_VALUE('<root/>', '/root', NULL),
                   XML_EXISTS('<root/>', '0'),
                   XML_EXISTS('<root/>', '1'),
                   XML_EXISTS('<root/>', 'string(/missing)'),
                   XML_VALUE('<root><item/><item/></root>', 'count(/root/item)'),
                   XML_VALUE('<root/>', '1 = 1'),
                   XML_VALUE('<root/>', 'string(/missing)'),
                   XML_VALUE('<root/>', '/missing')
            """,
            Ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.All(row[..3], static value => Assert.True(value.IsNull));
        Assert.Equal(0, row[3].AsInteger);
        Assert.Equal(1, row[4].AsInteger);
        Assert.Equal(0, row[5].AsInteger);
        Assert.Equal("2", row[6].AsText);
        Assert.Equal("true", row[7].AsText);
        Assert.Equal(string.Empty, row[8].AsText);
        Assert.True(row[9].IsNull);
    }

    [Fact]
    public async Task SystemFunctions_ExposesXmlFunctionsAndAlias()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await using QueryResult result = await database.ExecuteAsync(
            """
            SELECT function_name, canonical_name, signature, return_type,
                   null_behavior, volatility, is_deterministic, is_builtin
            FROM sys.functions
            WHERE function_name IN ('XML_EXISTS', 'XMLEXISTS', 'XML_VALUE')
            """,
            Ct);

        Dictionary<string, DbValue[]> rows = (await result.ToListAsync(Ct))
            .ToDictionary(static row => row[0].AsText, StringComparer.Ordinal);
        Assert.Equal(3, rows.Count);

        Assert.Equal("XML_EXISTS", rows["XML_EXISTS"][1].AsText);
        Assert.Equal("XML_EXISTS", rows["XMLEXISTS"][1].AsText);
        Assert.Equal("XML_EXISTS(2..3)", rows["XMLEXISTS"][2].AsText);
        Assert.Equal("INTEGER", rows["XML_EXISTS"][3].AsText);
        Assert.Equal("XML_VALUE", rows["XML_VALUE"][1].AsText);
        Assert.Equal("TEXT", rows["XML_VALUE"][3].AsText);
        Assert.All(rows.Values, static row =>
        {
            Assert.Equal("propagates", row[4].AsText);
            Assert.Equal("immutable", row[5].AsText);
            Assert.Equal(1, row[6].AsInteger);
            Assert.Equal(1, row[7].AsInteger);
        });
    }

    [Fact]
    public async Task XmlValue_RejectsMultiNodeResultsUnlessCallerChoosesScalarSemantics()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using QueryResult invalid = await database.ExecuteAsync(
                "SELECT XML_VALUE('<root><item>A</item><item>B</item></root>', '/root/item')",
                Ct);
            _ = await invalid.ToListAsync(Ct);
        });
        Assert.Equal(ErrorCode.TypeMismatch, error.Code);
        Assert.Contains("at most one node", error.Message, StringComparison.Ordinal);

        await using QueryResult valid = await database.ExecuteAsync(
            "SELECT XML_VALUE('<root><item>A</item><item>B</item></root>', " +
            "'string((/root/item)[1])')",
            Ct);
        Assert.Equal("A", Assert.Single(await valid.ToListAsync(Ct))[0].AsText);
    }

    [Fact]
    public void XmlCodec_RejectsUnsafeOrInvalidInputsWithStableErrorCodes()
    {
        CSharpDbException dtd = Assert.Throws<CSharpDbException>(() =>
            CSharpDbXmlCodec.Canonicalize(
                "<!DOCTYPE root [<!ENTITY value 'expanded'>]><root>&value;</root>"));
        Assert.Equal(ErrorCode.TypeMismatch, dtd.Code);
        Assert.Contains("well-formed document", dtd.Message, StringComparison.Ordinal);

        CSharpDbException xpath = Assert.Throws<CSharpDbException>(() =>
            CSharpDbXmlCodec.Exists("<root/>", "/root["));
        Assert.Equal(ErrorCode.SyntaxError, xpath.Code);
        Assert.Contains("Invalid XPath", xpath.Message, StringComparison.Ordinal);

        CSharpDbException prefix = Assert.Throws<CSharpDbException>(() =>
            CSharpDbXmlCodec.Exists("<root/>", "/o:root"));
        Assert.Equal(ErrorCode.SyntaxError, prefix.Code);

        CSharpDbException namespaces = Assert.Throws<CSharpDbException>(() =>
            CSharpDbXmlCodec.Exists("<root/>", "/root", "{\"\":\"urn:root\"}"));
        Assert.Equal(ErrorCode.SyntaxError, namespaces.Code);
    }

    [Fact]
    public async Task XmlCast_UsesSecureParsingForSafeAndUnsafeDocuments()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await using QueryResult valid = await database.ExecuteAsync(
            "SELECT CAST('<root>  <item>A</item>  </root>' AS XML)",
            Ct);
        Assert.Equal(
            "<root>  <item>A</item>  </root>",
            Assert.Single(await valid.ToListAsync(Ct))[0].AsText);

        CSharpDbException dtd = await Assert.ThrowsAsync<CSharpDbException>(async () =>
        {
            await using QueryResult invalid = await database.ExecuteAsync(
                "SELECT CAST('<!DOCTYPE root [<!ENTITY value \"expanded\">]><root>&value;</root>' AS XML)",
                Ct);
            _ = await invalid.ToListAsync(Ct);
        });
        Assert.Equal(ErrorCode.TypeMismatch, dtd.Code);
        Assert.Contains("well-formed document", dtd.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void XmlCodec_EnforcesXPathAndDocumentDepthLimits()
    {
        string longXPath = "/" + new string('a', CSharpDbXmlCodec.MaximumXPathCharacters);
        CSharpDbException pathLimit = Assert.Throws<CSharpDbException>(() =>
            CSharpDbXmlCodec.Exists("<root/>", longXPath));
        Assert.Equal(ErrorCode.ResourceLimitExceeded, pathLimit.Code);

        string deepXml = string.Concat(
            Enumerable.Repeat("<n>", CSharpDbXmlCodec.MaximumDocumentDepth + 1)) +
            string.Concat(Enumerable.Repeat("</n>", CSharpDbXmlCodec.MaximumDocumentDepth + 1));
        CSharpDbException depthLimit = Assert.Throws<CSharpDbException>(() =>
            CSharpDbXmlCodec.Canonicalize(deepXml));
        Assert.Equal(ErrorCode.ResourceLimitExceeded, depthLimit.Code);
    }
}
