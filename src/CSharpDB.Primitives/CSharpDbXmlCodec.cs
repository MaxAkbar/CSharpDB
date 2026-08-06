using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;

namespace CSharpDB.Primitives;

/// <summary>
/// Secure XML parsing and XPath 1.0 evaluation shared by the XML SQL type and
/// the built-in XML query functions.
/// </summary>
public static class CSharpDbXmlCodec
{
    public const int MaximumDocumentCharacters = 8 * 1024 * 1024;
    public const int MaximumDocumentNodes = 100_000;
    public const int MaximumDocumentDepth = 256;
    public const int MaximumXPathCharacters = 4_096;
    public const int MaximumXPathNesting = 64;
    public const int MaximumNamespaceMapCharacters = 16 * 1024;
    public const int MaximumNamespaceBindings = 32;
    public const int MaximumNamespacePrefixCharacters = 128;
    public const int MaximumNamespaceUriCharacters = 2_048;

    private const int XPathCacheCapacity = 256;
    private const string XmlNamespacePrefix = "xml";
    private const string XmlnsNamespacePrefix = "xmlns";
    private const string XmlNamespaceUri = "http://www.w3.org/XML/1998/namespace";

    private static readonly object s_xpathCacheLock = new();
    private static readonly Dictionary<XPathCacheKey, LinkedListNode<CachedXPath>> s_xpathCache =
        new();
    private static readonly LinkedList<CachedXPath> s_xpathLru = new();

    /// <summary>
    /// Validates a complete XML document and returns its stable compact
    /// serialization. DTDs and external resource resolution are prohibited.
    /// </summary>
    public static string Canonicalize(string xml)
    {
        ArgumentNullException.ThrowIfNull(xml);
        return ParseDocument(xml).ToString(SaveOptions.DisableFormatting);
    }

    /// <summary>
    /// Evaluates the XPath 1.0 effective boolean value of an expression.
    /// </summary>
    public static bool Exists(
        string xml,
        string xpath,
        string? namespaceMapJson = null)
    {
        XDocument document = ParseDocument(xml);
        XPathQuery query = PrepareQuery(xpath, namespaceMapJson);
        XPathExpression expression = GetCompiledExpression(
            $"boolean({query.Expression})",
            query);

        object result = Evaluate(document, expression);
        return result is bool exists
            ? exists
            : throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "XML_EXISTS did not produce an XPath boolean result.");
    }

    /// <summary>
    /// Evaluates an XPath 1.0 expression as a scalar string. An empty node-set
    /// returns null; a node-set with more than one item is rejected.
    /// </summary>
    public static string? Value(
        string xml,
        string xpath,
        string? namespaceMapJson = null)
    {
        XDocument document = ParseDocument(xml);
        XPathQuery query = PrepareQuery(xpath, namespaceMapJson);
        XPathExpression expression = GetCompiledExpression(query.Expression, query);
        object result = Evaluate(document, expression);

        if (result is XPathNodeIterator nodes)
        {
            if (!nodes.MoveNext())
                return null;

            XPathNavigator selected = nodes.Current?.Clone()
                ?? throw new CSharpDbException(
                    ErrorCode.TypeMismatch,
                    "XML_VALUE could not read the selected XPath node.");
            if (nodes.MoveNext())
            {
                throw new CSharpDbException(
                    ErrorCode.TypeMismatch,
                    "XML_VALUE requires an XPath expression that returns at most one node.");
            }

            return selected.Value;
        }

        if (result is string text)
            return text;

        if (result is bool or double)
        {
            XPathExpression stringExpression = GetCompiledExpression(
                $"string({query.Expression})",
                query);
            return Evaluate(document, stringExpression) as string
                ?? throw new CSharpDbException(
                    ErrorCode.TypeMismatch,
                    "XML_VALUE could not convert the XPath scalar result to text.");
        }

        throw new CSharpDbException(
            ErrorCode.TypeMismatch,
            $"XML_VALUE does not support XPath result type '{result.GetType().Name}'.");
    }

    private static XDocument ParseDocument(string xml)
    {
        ArgumentNullException.ThrowIfNull(xml);
        if (xml.Length > MaximumDocumentCharacters)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"XML input exceeds the {MaximumDocumentCharacters} character limit.");
        }

        var settings = new XmlReaderSettings
        {
            CheckCharacters = true,
            ConformanceLevel = ConformanceLevel.Document,
            DtdProcessing = DtdProcessing.Prohibit,
            IgnoreComments = false,
            IgnoreProcessingInstructions = false,
            IgnoreWhitespace = false,
            MaxCharactersInDocument = MaximumDocumentCharacters,
            ValidationType = ValidationType.None,
            XmlResolver = null,
        };

        try
        {
            using var input = new StringReader(xml);
            using XmlReader reader = new CountingXmlReader(XmlReader.Create(input, settings));
            return XDocument.Load(reader, LoadOptions.None);
        }
        catch (CSharpDbException)
        {
            throw;
        }
        catch (XmlException ex)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"XML input is not a well-formed document: {ex.Message}",
                ex);
        }
    }

    private static XPathQuery PrepareQuery(
        string xpath,
        string? namespaceMapJson)
    {
        ArgumentNullException.ThrowIfNull(xpath);
        if (string.IsNullOrWhiteSpace(xpath))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "XPath expression cannot be empty.");
        }
        if (xpath.Length > MaximumXPathCharacters)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"XPath expression exceeds the {MaximumXPathCharacters} character limit.");
        }

        ValidateXPathNesting(xpath);
        NamespaceBinding[] bindings = ParseNamespaceBindings(namespaceMapJson);
        string namespaceCacheKey = string.Concat(bindings.Select(static binding =>
            $"{binding.Prefix.Length}:{binding.Prefix}{binding.Uri.Length}:{binding.Uri}"));
        return new XPathQuery(xpath, bindings, namespaceCacheKey);
    }

    private static void ValidateXPathNesting(string xpath)
    {
        int nesting = 0;
        char quote = '\0';
        for (int i = 0; i < xpath.Length; i++)
        {
            char current = xpath[i];
            if (quote != '\0')
            {
                if (current == quote)
                    quote = '\0';
                continue;
            }

            if (current is '\'' or '"')
            {
                quote = current;
                continue;
            }

            if (current is '(' or '[')
            {
                nesting++;
                if (nesting > MaximumXPathNesting)
                {
                    throw new CSharpDbException(
                        ErrorCode.ResourceLimitExceeded,
                        $"XPath expression exceeds the maximum nesting of {MaximumXPathNesting}.");
                }
            }
            else if (current is ')' or ']')
            {
                nesting--;
            }
        }
    }

    private static NamespaceBinding[] ParseNamespaceBindings(string? namespaceMapJson)
    {
        if (namespaceMapJson is null)
            return [];
        if (namespaceMapJson.Length > MaximumNamespaceMapCharacters)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"XML namespace map exceeds the {MaximumNamespaceMapCharacters} character limit.");
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(
                namespaceMapJson,
                new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = 4,
                });
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw NamespaceSyntaxError(
                    "XML namespace map must be a JSON object of prefix-to-URI strings.");
            }

            var bindings = new List<NamespaceBinding>();
            var prefixes = new HashSet<string>(StringComparer.Ordinal);
            foreach (JsonProperty property in document.RootElement.EnumerateObject())
            {
                if (bindings.Count == MaximumNamespaceBindings)
                {
                    throw new CSharpDbException(
                        ErrorCode.ResourceLimitExceeded,
                        $"XML namespace map exceeds the {MaximumNamespaceBindings} binding limit.");
                }
                if (!prefixes.Add(property.Name))
                    throw NamespaceSyntaxError("XML namespace map contains a duplicate prefix.");
                if (property.Value.ValueKind != JsonValueKind.String)
                    throw NamespaceSyntaxError("Every XML namespace URI must be a JSON string.");

                string prefix = property.Name;
                string uri = property.Value.GetString()!;
                ValidateNamespaceBinding(prefix, uri);
                bindings.Add(new NamespaceBinding(prefix, uri));
            }

            return bindings
                .OrderBy(static binding => binding.Prefix, StringComparer.Ordinal)
                .ToArray();
        }
        catch (CSharpDbException)
        {
            throw;
        }
        catch (JsonException ex)
        {
            throw NamespaceSyntaxError(
                $"XML namespace map is not valid JSON: {ex.Message}",
                ex);
        }
    }

    private static void ValidateNamespaceBinding(string prefix, string uri)
    {
        if (prefix.Length == 0)
            throw NamespaceSyntaxError("XPath namespace prefixes cannot be empty.");
        if (prefix.Length > MaximumNamespacePrefixCharacters)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"XPath namespace prefix exceeds {MaximumNamespacePrefixCharacters} characters.");
        }
        if (uri.Length == 0)
            throw NamespaceSyntaxError("XPath namespace URIs cannot be empty.");
        if (uri.Length > MaximumNamespaceUriCharacters)
        {
            throw new CSharpDbException(
                ErrorCode.ResourceLimitExceeded,
                $"XPath namespace URI exceeds {MaximumNamespaceUriCharacters} characters.");
        }
        if (string.Equals(prefix, XmlnsNamespacePrefix, StringComparison.Ordinal))
            throw NamespaceSyntaxError("The reserved 'xmlns' prefix cannot be bound in XPath.");
        if (string.Equals(prefix, XmlNamespacePrefix, StringComparison.Ordinal) &&
            !string.Equals(uri, XmlNamespaceUri, StringComparison.Ordinal))
        {
            throw NamespaceSyntaxError(
                $"The reserved 'xml' prefix must map to '{XmlNamespaceUri}'.");
        }

        try
        {
            _ = XmlConvert.VerifyNCName(prefix);
        }
        catch (XmlException ex)
        {
            throw NamespaceSyntaxError("XPath namespace prefix is not a valid XML name.", ex);
        }
    }

    private static XPathExpression GetCompiledExpression(
        string expression,
        XPathQuery query)
    {
        var cacheKey = new XPathCacheKey(expression, query.NamespaceCacheKey);
        lock (s_xpathCacheLock)
        {
            if (s_xpathCache.TryGetValue(cacheKey, out LinkedListNode<CachedXPath>? cached))
            {
                s_xpathLru.Remove(cached);
                s_xpathLru.AddFirst(cached);
                return cached.Value.Expression.Clone();
            }
        }

        XPathExpression compiled;
        try
        {
            var namespaces = new XmlNamespaceManager(new NameTable());
            foreach (NamespaceBinding binding in query.NamespaceBindings)
                namespaces.AddNamespace(binding.Prefix, binding.Uri);
            compiled = XPathExpression.Compile(expression, namespaces);
        }
        catch (XPathException ex)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Invalid XPath expression: {ex.Message}",
                ex);
        }
        catch (ArgumentException ex)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Invalid XPath namespace context: {ex.Message}",
                ex);
        }

        lock (s_xpathCacheLock)
        {
            if (s_xpathCache.TryGetValue(cacheKey, out LinkedListNode<CachedXPath>? raced))
            {
                s_xpathLru.Remove(raced);
                s_xpathLru.AddFirst(raced);
                return raced.Value.Expression.Clone();
            }

            var entry = new LinkedListNode<CachedXPath>(new CachedXPath(cacheKey, compiled));
            s_xpathCache.Add(cacheKey, entry);
            s_xpathLru.AddFirst(entry);
            if (s_xpathCache.Count > XPathCacheCapacity)
            {
                LinkedListNode<CachedXPath> oldest = s_xpathLru.Last!;
                s_xpathLru.RemoveLast();
                s_xpathCache.Remove(oldest.Value.Key);
            }
        }

        return compiled.Clone();
    }

    private static object Evaluate(
        XDocument document,
        XPathExpression expression)
    {
        XPathNavigator navigator = document.CreateNavigator()
            ?? throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                "XML document could not be opened for XPath evaluation.");
        try
        {
            return navigator.Evaluate(expression);
        }
        catch (XPathException ex)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"XPath evaluation failed: {ex.Message}",
                ex);
        }
    }

    private static CSharpDbException NamespaceSyntaxError(
        string message,
        Exception? innerException = null) =>
        innerException is null
            ? new CSharpDbException(ErrorCode.SyntaxError, message)
            : new CSharpDbException(ErrorCode.SyntaxError, message, innerException);

    private sealed class CountingXmlReader(XmlReader inner) : XmlReader
    {
        private int _nodeCount;

        public override int AttributeCount => inner.AttributeCount;
        public override string BaseURI => inner.BaseURI;
        public override int Depth => inner.Depth;
        public override bool EOF => inner.EOF;
        public override bool IsEmptyElement => inner.IsEmptyElement;
        public override string LocalName => inner.LocalName;
        public override string NamespaceURI => inner.NamespaceURI;
        public override XmlNameTable NameTable => inner.NameTable;
        public override XmlNodeType NodeType => inner.NodeType;
        public override string Prefix => inner.Prefix;
        public override ReadState ReadState => inner.ReadState;
        public override string Value => inner.Value;

        public override string? GetAttribute(string name) => inner.GetAttribute(name);
        public override string GetAttribute(int i) => inner.GetAttribute(i);
        public override string? GetAttribute(string name, string? namespaceURI) =>
            inner.GetAttribute(name, namespaceURI);
        public override string? LookupNamespace(string prefix) => inner.LookupNamespace(prefix);
        public override bool MoveToAttribute(string name) => inner.MoveToAttribute(name);
        public override bool MoveToAttribute(string name, string? ns) =>
            inner.MoveToAttribute(name, ns);
        public override bool MoveToElement() => inner.MoveToElement();
        public override bool MoveToFirstAttribute() => inner.MoveToFirstAttribute();
        public override bool MoveToNextAttribute() => inner.MoveToNextAttribute();
        public override bool ReadAttributeValue() => inner.ReadAttributeValue();
        public override void ResolveEntity() => inner.ResolveEntity();

        public override bool Read()
        {
            if (!inner.Read())
                return false;

            int addedNodes = inner.NodeType switch
            {
                XmlNodeType.None or XmlNodeType.EndElement or XmlNodeType.XmlDeclaration => 0,
                XmlNodeType.Element => checked(1 + inner.AttributeCount),
                _ => 1,
            };
            _nodeCount = checked(_nodeCount + addedNodes);
            if (_nodeCount > MaximumDocumentNodes)
            {
                throw new CSharpDbException(
                    ErrorCode.ResourceLimitExceeded,
                    $"XML input exceeds the {MaximumDocumentNodes} node limit.");
            }

            if (addedNodes > 0 && checked(inner.Depth + 1) > MaximumDocumentDepth)
            {
                throw new CSharpDbException(
                    ErrorCode.ResourceLimitExceeded,
                    $"XML input exceeds the maximum depth of {MaximumDocumentDepth}.");
            }

            return true;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }

    private sealed record CachedXPath(XPathCacheKey Key, XPathExpression Expression);

    private readonly record struct XPathCacheKey(
        string Expression,
        string NamespaceKey);

    private sealed record XPathQuery(
        string Expression,
        NamespaceBinding[] NamespaceBindings,
        string NamespaceCacheKey);

    private readonly record struct NamespaceBinding(string Prefix, string Uri);
}
