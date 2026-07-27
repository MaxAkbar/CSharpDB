using System.Security.Cryptography;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public sealed class StandardDataTypeMappingProvider : IDataTypeMappingProvider
{
    public const string StandardPolicyId = "csharpdb-standard-mapping";
    public const int StandardPolicyVersion = 1;

    private const string JsonTypedSchemaContract =
        "csharpdb-json-typed-table-schema-v1";
    private const string JsonTypedScalarContract =
        "csharpdb-json-typed-table-scalar-v1";
    private const string JsonTypedValueContract =
        "csharpdb-json-typed-value/v1";

    internal const string LossyRuleId = "MIG-TYPE-LOSSY-001";
    internal const string UnsupportedRuleId = "MIG-TYPE-UNSUPPORTED-001";

    public string PolicyId => StandardPolicyId;

    public int PolicyVersion => StandardPolicyVersion;

    public MigrationTypeMappingDecision Map(MigrationTypeMappingRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.SourceObject);
        ArgumentNullException.ThrowIfNull(request.Coverage);
        if (string.IsNullOrWhiteSpace(request.SourceObject.NativeType))
            throw new ArgumentException("A mapped source object must have a native type.", nameof(request));
        if (request.Profile != MigrationMappingProfile.Custom && request.CustomTargetType is not null)
            throw new ArgumentException("A custom target type requires the custom mapping profile.", nameof(request));

        string logicalType = GetFacet(request.SourceObject, "logicalType") ??
            request.SourceObject.NativeType!;
        MappingShape shape = request.Profile switch
        {
            MigrationMappingProfile.Preserve => Preserve(logicalType, request.SourceObject),
            MigrationMappingProfile.Queryable => Queryable(logicalType, request.SourceObject),
            MigrationMappingProfile.Custom => request.CustomTargetType is DbType targetType
                ? Custom(logicalType, targetType, request.SourceObject)
                : Preserve(logicalType, request.SourceObject),
            _ => throw new ArgumentOutOfRangeException(nameof(request), "Unknown mapping profile."),
        };

        string? diagnosticId = shape.Classification is MigrationMappingClassification.Lossy or
            MigrationMappingClassification.Unsupported
            ? CreateDiagnosticId(
                shape.Classification == MigrationMappingClassification.Lossy ? LossyRuleId : UnsupportedRuleId,
                request.SourceObject.ObjectId,
                shape.TargetType,
                shape.Conversion)
            : null;

        var mapping = new MigrationTypeMapping
        {
            SourceObjectId = request.SourceObject.ObjectId,
            SourceNativeType = request.SourceObject.NativeType!,
            TargetType = shape.TargetType,
            RequestedTargetType = request.CustomTargetType,
            Classification = shape.Classification,
            Profile = request.Profile,
            Coverage = request.Coverage,
            Conversion = shape.Conversion,
            DiagnosticId = diagnosticId,
        };

        MigrationDiagnostic? diagnostic = diagnosticId is null
            ? null
            : CreateDiagnostic(mapping, diagnosticId);
        return new MigrationTypeMappingDecision
        {
            Mapping = mapping,
            Diagnostic = diagnostic,
        };
    }

    internal static bool IsTrustedLossyDiagnostic(
        MigrationTypeMapping mapping,
        MigrationDiagnostic diagnostic)
    {
        if (mapping.Classification != MigrationMappingClassification.Lossy ||
            !string.Equals(diagnostic.RuleId, LossyRuleId, StringComparison.Ordinal) ||
            !diagnostic.CanOverride ||
            diagnostic.Status != MigrationCompatibilityStatus.Conditional ||
            !string.Equals(diagnostic.ObjectId, mapping.SourceObjectId, StringComparison.Ordinal))
        {
            return false;
        }

        string expectedId = CreateDiagnosticId(
            LossyRuleId,
            mapping.SourceObjectId,
            mapping.TargetType,
            mapping.Conversion);
        return string.Equals(diagnostic.DiagnosticId, expectedId, StringComparison.Ordinal);
    }

    private static MappingShape Preserve(string logicalType, MigrationCatalogObject source) =>
        logicalType.ToUpperInvariant() switch
        {
            "SIGNEDINTEGER" => Exact(DbType.Integer),
            "TEXT" => Exact(DbType.Text),
            "BINARY" => Exact(DbType.Blob),
            "FLOATINGPOINT" => Exact(DbType.Real),
            "JSON" when IsOrderedJsonDocument(source) => Reencoded(
                DbType.Text,
                "canonical-text",
                Facet("logicalType", MigrationDocumentCollectionContract.JsonLogicalType)),
            "BOOLEAN" => Reencoded(DbType.Integer, "boolean-integer", Facet("true", "1"), Facet("false", "0")),
            "GUID" => Reencoded(DbType.Text, "guid-text", TextCodecParameters(CSharpDbTextCodec.GuidFormat)),
            "DATE" => Reencoded(DbType.Text, "date-text", TextCodecParameters(CSharpDbTextCodec.DateFormat)),
            "TIME" => Reencoded(DbType.Text, "time-text", TimeCodecParameters()),
            "DATETIME" => Reencoded(DbType.Text, "datetime-text", TextCodecParameters(CSharpDbTextCodec.DateTimeFormat)),
            "DATETIMEOFFSET" => Reencoded(DbType.Text, "datetimeoffset-text", TextCodecParameters(CSharpDbTextCodec.DateTimeOffsetFormat)),
            "DECIMAL" => PreserveDecimal(source),
            "UNSIGNEDINTEGER" => Reencoded(DbType.Text, "unsigned-integer-text", Facet("format", "invariant-base10")),
            _ => Unsupported(),
        };

    private static MappingShape Queryable(string logicalType, MigrationCatalogObject source) =>
        logicalType.ToUpperInvariant() switch
        {
            "DECIMAL" => QueryableDecimal(source),
            "UNSIGNEDINTEGER" => Lossy(DbType.Real, "unsigned-integer-binary64", Facet("format", "ieee754-binary64")),
            _ => Preserve(logicalType, source),
        };

    private static MappingShape Custom(
        string logicalType,
        DbType targetType,
        MigrationCatalogObject source)
    {
        string logical = logicalType.ToUpperInvariant();
        return targetType switch
        {
            DbType.Integer when logical == "SIGNEDINTEGER" => Exact(DbType.Integer),
            DbType.Integer when logical == "BOOLEAN" =>
                Reencoded(DbType.Integer, "boolean-integer", Facet("true", "1"), Facet("false", "0")),
            DbType.Integer when logical == "DECIMAL" => PreserveDecimal(source) is
                { TargetType: DbType.Integer } scaled
                    ? scaled
                    : Unsupported(),
            DbType.Real when logical == "FLOATINGPOINT" => Exact(DbType.Real),
            DbType.Real when logical is "SIGNEDINTEGER" or "UNSIGNEDINTEGER" or "DECIMAL" =>
                Lossy(DbType.Real, "numeric-binary64", Facet("format", "ieee754-binary64")),
            DbType.Text when logical == "TEXT" => Exact(DbType.Text),
            DbType.Text when logical == "DECIMAL" =>
                IsTypedJsonDecimal(source)
                    ? Reencoded(
                        DbType.Text,
                        "json-typed-decimal-text",
                        TypedJsonDecimalTextParameters(source))
                    : Reencoded(
                        DbType.Text,
                        "decimal-text",
                        DecimalTextParameters(source)),
            DbType.Text when logical is not ("BINARY" or "GEOGRAPHY") =>
                Reencoded(DbType.Text, "canonical-text", Facet("logicalType", logicalType)),
            DbType.Blob when logical == "BINARY" => Exact(DbType.Blob),
            _ => Unsupported(),
        };
    }

    private static MigrationDiagnostic CreateDiagnostic(
        MigrationTypeMapping mapping,
        string diagnosticId)
    {
        bool lossy = mapping.Classification == MigrationMappingClassification.Lossy;
        return new MigrationDiagnostic
        {
            DiagnosticId = diagnosticId,
            RuleId = lossy ? LossyRuleId : UnsupportedRuleId,
            Severity = lossy ? MigrationDiagnosticSeverity.Warning : MigrationDiagnosticSeverity.Error,
            Status = lossy
                ? MigrationCompatibilityStatus.Conditional
                : MigrationCompatibilityStatus.Unsupported,
            Evidence = MigrationEvidenceLevel.CapabilityMatched,
            Summary = lossy
                ? $"{mapping.SourceNativeType} requires a lossy target representation."
                : $"{mapping.SourceNativeType} has no supported target representation.",
            Explanation = lossy
                ? $"The selected {mapping.Profile} profile maps this value to {mapping.TargetType}; exact source values may not round-trip."
                : "No registered, reproducible conversion preserves this source-native value in a CSharpDB column.",
            ObjectId = mapping.SourceObjectId,
            Remediation = lossy
                ? "Choose the preserve profile or explicitly accept this diagnostic after reviewing profiled coverage."
                : "Exclude the object or provide a versioned custom conversion.",
            CanOverride = lossy,
        };
    }

    private static string CreateDiagnosticId(
        string ruleId,
        string objectId,
        DbType? targetType,
        MigrationConversionDescriptor? conversion)
    {
        string input = string.Join(
            "|",
            ruleId,
            objectId,
            targetType?.ToString() ?? "none",
            conversion?.ConversionId ?? "none",
            conversion?.Version.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none");
        string hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)))
            .ToLowerInvariant()[..16];
        return $"diag:{ruleId.ToLowerInvariant()}:{hash}";
    }

    private static MappingShape Exact(DbType targetType) =>
        new(targetType, MigrationMappingClassification.Exact, null);

    private static MappingShape Reencoded(
        DbType targetType,
        string conversionId,
        params MigrationCatalogFacet[] parameters) =>
        new(targetType, MigrationMappingClassification.LosslessReencoded, Conversion(conversionId, parameters));

    private static MappingShape Reencoded(
        DbType targetType,
        string conversionId,
        IReadOnlyList<MigrationCatalogFacet> parameters) =>
        new(targetType, MigrationMappingClassification.LosslessReencoded, Conversion(conversionId, parameters));

    private static MappingShape Lossy(
        DbType targetType,
        string conversionId,
        params MigrationCatalogFacet[] parameters) =>
        new(targetType, MigrationMappingClassification.Lossy, Conversion(conversionId, parameters));

    private static MappingShape Lossy(
        DbType targetType,
        string conversionId,
        IReadOnlyList<MigrationCatalogFacet> parameters) =>
        new(targetType, MigrationMappingClassification.Lossy, Conversion(conversionId, parameters));

    private static MappingShape Unsupported() =>
        new(null, MigrationMappingClassification.Unsupported, null);

    private static MappingShape PreserveDecimal(MigrationCatalogObject source)
    {
        if (TryGetScaledDecimalFacets(source, out int precision, out int scale))
        {
            return Reencoded(
                DbType.Integer,
                "decimal-scaled-int64",
                ScaledDecimalParameters(precision, scale));
        }

        if (IsTypedJsonDecimal(source))
        {
            return Reencoded(
                DbType.Text,
                "json-typed-decimal-text",
                TypedJsonDecimalTextParameters(source));
        }

        return Reencoded(DbType.Text, "decimal-text", DecimalTextParameters(source));
    }

    private static MappingShape QueryableDecimal(MigrationCatalogObject source)
    {
        if (TryGetScaledDecimalFacets(source, out int precision, out int scale))
        {
            return Reencoded(
                DbType.Integer,
                "decimal-scaled-int64",
                ScaledDecimalParameters(precision, scale));
        }

        return Lossy(DbType.Real, "decimal-binary64", DecimalTextParameters(source));
    }

    private static MigrationConversionDescriptor Conversion(
        string conversionId,
        IReadOnlyList<MigrationCatalogFacet> parameters) => new()
    {
        ConversionId = conversionId,
        Version = 1,
        Parameters = parameters,
    };

    private static IReadOnlyList<MigrationCatalogFacet> DecimalTextParameters(MigrationCatalogObject source) =>
    [
        Facet("format", "invariant-base10"),
        Facet("precision", GetFacet(source, "precision") ?? "unspecified"),
        Facet("scale", GetFacet(source, "scale") ?? "unspecified"),
    ];

    private static IReadOnlyList<MigrationCatalogFacet>
        TypedJsonDecimalTextParameters(
            MigrationCatalogObject source) =>
    [
        Facet("format", "canonical-fixed-point"),
        Facet(
            "precision",
            GetFacet(source, "precision") ??
            "unspecified"),
        Facet(
            "scale",
            GetFacet(source, "scale") ??
            "unspecified"),
        Facet(
            "contract",
            "csharpdb-json-typed-value/v1"),
    ];

    private static IReadOnlyList<MigrationCatalogFacet> ScaledDecimalParameters(
        int precision,
        int scale) =>
    [
        Facet("codec", nameof(CSharpDbDecimalCodec)),
        Facet("codecVersion", CSharpDbDecimalCodec.Version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        Facet("precision", precision.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        Facet("scale", scale.ToString(System.Globalization.CultureInfo.InvariantCulture)),
    ];

    private static IReadOnlyList<MigrationCatalogFacet> TextCodecParameters(string format) =>
    [
        Facet("codec", nameof(CSharpDbTextCodec)),
        Facet("codecVersion", CSharpDbTextCodec.Version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        Facet("format", format),
    ];

    private static IReadOnlyList<MigrationCatalogFacet> TimeCodecParameters() =>
    [
        Facet("codec", nameof(CSharpDbTextCodec)),
        Facet("codecVersion", CSharpDbTextCodec.Version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        Facet("fractionalFormat", CSharpDbTextCodec.TimeFractionalFormat),
        Facet("integralFormat", CSharpDbTextCodec.TimeFormat),
    ];

    private static bool TryGetScaledDecimalFacets(
        MigrationCatalogObject source,
        out int precision,
        out int scale)
    {
        bool precisionParsed = int.TryParse(
            GetFacet(source, "precision"),
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture,
            out precision);
        bool scaleParsed = int.TryParse(
            GetFacet(source, "scale"),
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture,
            out scale);
        if (!precisionParsed || !scaleParsed)
            return false;

        try
        {
            CSharpDbDecimalCodec.ValidateFacets(precision, scale);
            return true;
        }
        catch (NotSupportedException)
        {
            return false;
        }
    }

    private static bool IsTypedJsonDecimal(
        MigrationCatalogObject source)
    {
        string? codec = GetFacet(
            source,
            "jsonTypedCodec");
        string? expectedNativeType = codec switch
        {
            "decimalString" =>
                "JSON_DECIMAL_STRING",
            "decimalNumber" =>
                "JSON_DECIMAL_NUMBER",
            _ => null,
        };
        return expectedNativeType is not null &&
            string.Equals(
                source.NativeType,
                expectedNativeType,
                StringComparison.Ordinal) &&
            string.Equals(
                GetFacet(source, "jsonSchemaAlgorithm"),
                JsonTypedSchemaContract,
                StringComparison.Ordinal) &&
            string.Equals(
                GetFacet(source, "jsonScalarPolicy"),
                JsonTypedScalarContract,
                StringComparison.Ordinal) &&
            string.Equals(
                GetFacet(source, "jsonTypedValueContract"),
                JsonTypedValueContract,
                StringComparison.Ordinal) &&
            string.Equals(
                GetFacet(source, "jsonTypedValidation"),
                "full-stream",
                StringComparison.Ordinal) &&
            !string.IsNullOrWhiteSpace(
                GetFacet(
                    source,
                    "jsonTypedIntentManifestDigest"));
    }

    private static bool IsOrderedJsonDocument(
        MigrationCatalogObject source) =>
        MigrationDocumentCollectionContract.IsSupportedV1DocumentColumn(
            source);

    private static MigrationCatalogFacet Facet(string name, string value) => new()
    {
        Name = name,
        Value = value,
    };

    private static string? GetFacet(MigrationCatalogObject source, string name) =>
        source.Facets.FirstOrDefault(facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private sealed record MappingShape(
        DbType? TargetType,
        MigrationMappingClassification Classification,
        MigrationConversionDescriptor? Conversion);
}
