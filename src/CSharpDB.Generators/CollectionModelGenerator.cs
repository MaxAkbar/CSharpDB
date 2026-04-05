using System;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace CSharpDB.Generators;

[Generator(LanguageNames.CSharp)]
public sealed class CollectionModelGenerator : IIncrementalGenerator
{
    private const string CollectionModelAttributeName = "CSharpDB.Engine.CollectionModelAttribute";
    private const string JsonIgnoreAttributeName = "System.Text.Json.Serialization.JsonIgnoreAttribute";
    private const string JsonPropertyNameAttributeName = "System.Text.Json.Serialization.JsonPropertyNameAttribute";

    private static readonly DiagnosticDescriptor TypeMustBePartial = new(
        id: "CDBGEN001",
        title: "Collection model types must be partial",
        messageFormat: "Type '{0}' must be declared partial to receive generated collection members",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor TypeMustBeTopLevel = new(
        id: "CDBGEN002",
        title: "Collection model types must be top-level",
        messageFormat: "Type '{0}' must be a top-level type to receive generated collection members",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor GenericTypesNotSupported = new(
        id: "CDBGEN003",
        title: "Generic collection model types are not supported",
        messageFormat: "Type '{0}' cannot be generic when using [CollectionModel]",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ReservedCollectionNameConflict = new(
        id: "CDBGEN004",
        title: "Collection member name is reserved",
        messageFormat: "Type '{0}' already declares a member named 'Collection', which conflicts with generated collection members",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor JsonContextTypeRequired = new(
        id: "CDBGEN005",
        title: "Collection model requires a JsonSerializerContext type",
        messageFormat: "Type '{0}' must specify a JsonSerializerContext type in [CollectionModel(typeof(...))]",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<CollectionGenerationResult> candidates =
            context.SyntaxProvider.ForAttributeWithMetadataName(
                fullyQualifiedMetadataName: CollectionModelAttributeName,
                predicate: static (node, _) => node is TypeDeclarationSyntax,
                transform: static (ctx, _) => InspectTarget(ctx))
            .Where(static result => result.Target is not null || result.Diagnostic is not null);

        context.RegisterSourceOutput(
            candidates,
            static (productionContext, result) =>
            {
                if (result.Diagnostic is not null)
                {
                    productionContext.ReportDiagnostic(result.Diagnostic);
                    return;
                }

                EmitModel(productionContext, result.Target!);
            });
    }

    private static CollectionGenerationResult InspectTarget(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetNode is not TypeDeclarationSyntax typeSyntax ||
            context.TargetSymbol is not INamedTypeSymbol typeSymbol)
        {
            return default;
        }

        Location location = typeSyntax.Identifier.GetLocation();
        if (!typeSyntax.Modifiers.Any(static modifier => modifier.IsKind(SyntaxKind.PartialKeyword)))
        {
            return new CollectionGenerationResult(
                null,
                Diagnostic.Create(TypeMustBePartial, location, typeSymbol.ToDisplayString()));
        }

        if (typeSymbol.ContainingType is not null)
        {
            return new CollectionGenerationResult(
                null,
                Diagnostic.Create(TypeMustBeTopLevel, location, typeSymbol.ToDisplayString()));
        }

        if (typeSymbol.Arity != 0)
        {
            return new CollectionGenerationResult(
                null,
                Diagnostic.Create(GenericTypesNotSupported, location, typeSymbol.ToDisplayString()));
        }

        if (typeSymbol.GetMembers("Collection").Any(static member => !member.IsImplicitlyDeclared))
        {
            return new CollectionGenerationResult(
                null,
                Diagnostic.Create(ReservedCollectionNameConflict, location, typeSymbol.ToDisplayString()));
        }

        INamedTypeSymbol? jsonContextType = context.Attributes
            .Select(static attribute => attribute.ConstructorArguments)
            .Where(static args => args.Length == 1)
            .Select(static args => args[0].Value as INamedTypeSymbol)
            .FirstOrDefault(static symbol => symbol is not null);
        if (jsonContextType is null)
        {
            return new CollectionGenerationResult(
                null,
                Diagnostic.Create(JsonContextTypeRequired, location, typeSymbol.ToDisplayString()));
        }

        var fields = ImmutableArray.CreateBuilder<CollectionFieldSpec>();
        foreach (ISymbol member in typeSymbol.GetMembers().OrderBy(static member => member.Name, StringComparer.Ordinal))
        {
            if (!SymbolEqualityComparer.Default.Equals(member.ContainingType, typeSymbol))
                continue;

            if (TryCreateField(member, out CollectionFieldSpec spec))
                fields.Add(spec);
        }

        string? namespaceName = typeSymbol.ContainingNamespace.IsGlobalNamespace
            ? null
            : typeSymbol.ContainingNamespace.ToDisplayString();

        return new CollectionGenerationResult(
            new CollectionModelTarget(
                namespaceName,
                typeSymbol.Name,
                typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                jsonContextType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                GetPartialTypeKeyword(typeSymbol),
                MakeSafeIdentifier(typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)),
                fields.ToImmutable()),
            null);
    }

    private static bool TryCreateField(ISymbol member, out CollectionFieldSpec field)
    {
        field = default;

        if (member.IsStatic || member.DeclaredAccessibility != Accessibility.Public || member.IsImplicitlyDeclared)
            return false;

        if (HasAttribute(member, JsonIgnoreAttributeName) || HasAttribute(member, JsonPropertyNameAttributeName))
            return false;

        ITypeSymbol? memberType = member switch
        {
            IPropertySymbol property when !property.IsIndexer &&
                                        property.GetMethod is not null &&
                                        property.GetMethod.DeclaredAccessibility == Accessibility.Public =>
                property.Type,
            IFieldSymbol fieldSymbol => fieldSymbol.Type,
            _ => null,
        };

        if (memberType is null || !TryClassifyFieldType(memberType, out string dataKind, out bool isMultiValue))
            return false;

        string fieldPath = isMultiValue ? member.Name + "[]" : member.Name;
        field = new CollectionFieldSpec(
            member.Name,
            EscapeIdentifier(member.Name),
            fieldPath,
            memberType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            dataKind);
        return true;
    }

    private static bool TryClassifyFieldType(ITypeSymbol type, out string dataKindName, out bool isMultiValue)
    {
        isMultiValue = false;
        ITypeSymbol effectiveType = UnwrapNullable(type);
        if (TryGetCollectionElementType(type, out ITypeSymbol? elementType))
        {
            isMultiValue = true;
            effectiveType = UnwrapNullable(elementType!);
        }

        if (effectiveType.SpecialType == SpecialType.System_String ||
            IsWellKnownType(effectiveType, "System.Guid") ||
            IsWellKnownType(effectiveType, "System.DateOnly") ||
            IsWellKnownType(effectiveType, "System.TimeOnly"))
        {
            dataKindName = "Text";
            return true;
        }

        if (effectiveType.TypeKind == TypeKind.Enum ||
            effectiveType.SpecialType is SpecialType.System_Byte or
                SpecialType.System_SByte or
                SpecialType.System_Int16 or
                SpecialType.System_UInt16 or
                SpecialType.System_Int32 or
                SpecialType.System_UInt32 or
                SpecialType.System_Int64 or
                SpecialType.System_UInt64)
        {
            dataKindName = "Integer";
            return true;
        }

        dataKindName = string.Empty;
        return false;
    }

    private static bool TryGetCollectionElementType(ITypeSymbol type, out ITypeSymbol? elementType)
    {
        elementType = null;
        if (type.SpecialType == SpecialType.System_String)
            return false;

        if (type is IArrayTypeSymbol arrayType)
        {
            elementType = arrayType.ElementType;
            return true;
        }

        if (type is not INamedTypeSymbol namedType)
            return false;

        if (namedType.IsGenericType &&
            namedType.TypeArguments.Length == 1 &&
            IsEnumerableLike(namedType.ConstructedFrom))
        {
            elementType = namedType.TypeArguments[0];
            return true;
        }

        foreach (INamedTypeSymbol interfaceType in namedType.AllInterfaces)
        {
            if (interfaceType.IsGenericType &&
                interfaceType.TypeArguments.Length == 1 &&
                IsEnumerableLike(interfaceType.ConstructedFrom))
            {
                elementType = interfaceType.TypeArguments[0];
                return true;
            }
        }

        return false;
    }

    private static bool IsEnumerableLike(INamedTypeSymbol constructedType)
    {
        string displayName = constructedType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return displayName is
            "global::System.Collections.Generic.IEnumerable<T>" or
            "global::System.Collections.Generic.ICollection<T>" or
            "global::System.Collections.Generic.IList<T>" or
            "global::System.Collections.Generic.IReadOnlyCollection<T>" or
            "global::System.Collections.Generic.IReadOnlyList<T>" or
            "global::System.Collections.Generic.List<T>";
    }

    private static ITypeSymbol UnwrapNullable(ITypeSymbol type)
        => type is INamedTypeSymbol named &&
           named.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
           named.TypeArguments.Length == 1
            ? named.TypeArguments[0]
            : type;

    private static bool HasAttribute(ISymbol symbol, string attributeTypeName)
        => symbol.GetAttributes().Any(attribute =>
            string.Equals(
                attribute.AttributeClass?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                "global::" + attributeTypeName,
                StringComparison.Ordinal));

    private static bool IsWellKnownType(ITypeSymbol type, string fullyQualifiedMetadataName)
        => string.Equals(
            type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            "global::" + fullyQualifiedMetadataName,
            StringComparison.Ordinal);

    private static string GetPartialTypeKeyword(INamedTypeSymbol type)
        => type switch
        {
            { IsRecord: true, TypeKind: TypeKind.Struct } => "record struct",
            { IsRecord: true } => "record",
            { TypeKind: TypeKind.Struct } => "struct",
            _ => "class",
        };

    private static string EscapeIdentifier(string identifier)
        => SyntaxFacts.GetKeywordKind(identifier) != SyntaxKind.None ||
           SyntaxFacts.GetContextualKeywordKind(identifier) != SyntaxKind.None
            ? "@" + identifier
            : identifier;

    private static string MakeSafeIdentifier(string input)
    {
        var builder = new StringBuilder(input.Length);
        foreach (char ch in input)
        {
            builder.Append(char.IsLetterOrDigit(ch) ? ch : '_');
        }

        return builder.ToString();
    }

    private static void EmitModel(SourceProductionContext context, CollectionModelTarget target)
    {
        var source = new StringBuilder();
        source.AppendLine("// <auto-generated />");
        source.AppendLine("#nullable enable");
        if (!string.IsNullOrEmpty(target.NamespaceName))
        {
            source.Append("namespace ").Append(target.NamespaceName).AppendLine(";");
            source.AppendLine();
        }

        source.AppendLine("[global::System.CodeDom.Compiler.GeneratedCode(\"CSharpDB.CollectionModelGenerator\", \"3.0.0\")]");
        source.Append("partial ").Append(target.PartialTypeKeyword).Append(' ').Append(target.TypeName).AppendLine();
        source.AppendLine("{");
        source.AppendLine("    public static partial class Collection");
        source.AppendLine("    {");

        foreach (CollectionFieldSpec field in target.Fields)
        {
            source.Append("        public static global::CSharpDB.Engine.CollectionField<")
                .Append(target.FullyQualifiedTypeName)
                .Append(", ")
                .Append(field.MemberTypeName)
                .Append("> ")
                .Append(field.EscapedMemberName)
                .AppendLine(" { get; } =");
            source.Append("            new(")
                .Append(SymbolDisplay.FormatLiteral(field.FieldPath, quote: true))
                .Append(", static document => document.")
                .Append(field.EscapedMemberName)
                .Append(", global::CSharpDB.Engine.CollectionIndexDataKind.")
                .Append(field.DataKindName)
                .AppendLine(");");
            source.AppendLine();
        }

        source.Append("        internal static bool TryGetField(string fieldPath, [global::System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out global::CSharpDB.Engine.CollectionField<")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(">? field)");
        source.AppendLine("        {");

        foreach (CollectionFieldSpec field in target.Fields)
        {
            source.Append("            if (global::System.StringComparer.OrdinalIgnoreCase.Equals(fieldPath, ")
                .Append(SymbolDisplay.FormatLiteral(field.FieldPath, quote: true))
                .AppendLine("))");
            source.AppendLine("            {");
            source.Append("                field = ").Append(field.EscapedMemberName).AppendLine(";");
            source.AppendLine("                return true;");
            source.AppendLine("            }");
        }

        source.AppendLine("            field = null;");
        source.AppendLine("            return false;");
        source.AppendLine("        }");
        source.AppendLine("    }");
        source.AppendLine("}");
        source.AppendLine();

        source.AppendLine("[global::System.CodeDom.Compiler.GeneratedCode(\"CSharpDB.CollectionModelGenerator\", \"3.0.0\")]");
        source.Append("internal sealed class __CSharpDB_CollectionModel_")
            .Append(target.SafeIdentifier)
            .Append(" : global::CSharpDB.Engine.ICollectionModel<")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(">");
        source.AppendLine("{");
        source.Append("    public global::CSharpDB.Engine.ICollectionDocumentCodec<")
            .Append(target.FullyQualifiedTypeName)
            .Append("> CreateCodec(global::CSharpDB.Storage.Serialization.IRecordSerializer recordSerializer)")
            .AppendLine();
        source.Append("        => new __CSharpDB_CollectionCodec_")
            .Append(target.SafeIdentifier)
            .AppendLine("(recordSerializer);");
        source.AppendLine();
        source.Append("    public bool TryGetField(string fieldPath, [global::System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out global::CSharpDB.Engine.CollectionField<")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(">? field)");
        source.Append("        => ")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(".Collection.TryGetField(fieldPath, out field);");
        source.AppendLine("}");
        source.AppendLine();

        source.AppendLine("[global::System.CodeDom.Compiler.GeneratedCode(\"CSharpDB.CollectionModelGenerator\", \"3.0.0\")]");
        source.Append("internal sealed class __CSharpDB_CollectionCodec_")
            .Append(target.SafeIdentifier)
            .Append(" : global::CSharpDB.Engine.ICollectionDocumentCodec<")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(">");
        source.AppendLine("{");
        source.AppendLine("    private const int StackallocKeyThreshold = 256;");
        source.AppendLine("    private readonly global::CSharpDB.Storage.Serialization.IRecordSerializer _recordSerializer;");
        source.AppendLine("    private readonly bool _usesDirectPayloadFormat;");
        source.Append("    private static readonly global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<")
            .Append(target.FullyQualifiedTypeName)
            .Append("> s_typeInfo = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<")
            .Append(target.FullyQualifiedTypeName)
            .Append(">)(")
            .Append(target.JsonContextTypeName)
            .Append(".Default.GetTypeInfo(typeof(")
            .Append(target.FullyQualifiedTypeName)
            .Append(")) ?? throw new global::System.InvalidOperationException(\"Generated collection JsonTypeInfo was not available.\"));")
            .AppendLine();
        source.AppendLine();
        source.Append("    public __CSharpDB_CollectionCodec_")
            .Append(target.SafeIdentifier)
            .AppendLine("(global::CSharpDB.Storage.Serialization.IRecordSerializer recordSerializer)");
        source.AppendLine("    {");
        source.AppendLine("        _recordSerializer = recordSerializer ?? throw new global::System.ArgumentNullException(nameof(recordSerializer));");
        source.AppendLine("        _usesDirectPayloadFormat = recordSerializer is global::CSharpDB.Storage.Serialization.DefaultRecordSerializer;");
        source.AppendLine("    }");
        source.AppendLine();
        source.Append("    public byte[] Encode(string key, ")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(" document)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(key);");
        source.AppendLine();
        source.AppendLine("        if (_usesDirectPayloadFormat)");
        source.AppendLine("        {");
        source.AppendLine("            byte[] jsonUtf8 = global::System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(document, s_typeInfo);");
        source.AppendLine("            return global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.Encode(key, jsonUtf8);");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        string json = global::System.Text.Json.JsonSerializer.Serialize(document, s_typeInfo);");
        source.AppendLine("        return _recordSerializer.Encode(new global::CSharpDB.Primitives.DbValue[]");
        source.AppendLine("        {");
        source.AppendLine("            global::CSharpDB.Primitives.DbValue.FromText(key),");
        source.AppendLine("            global::CSharpDB.Primitives.DbValue.FromText(json),");
        source.AppendLine("        });");
        source.AppendLine("    }");
        source.AppendLine();
        source.Append("    public (string Key, ")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(" Document) Decode(global::System.ReadOnlySpan<byte> payload)");
        source.AppendLine("        => (DecodeKey(payload), DecodeDocument(payload));");
        source.AppendLine();
        source.Append("    public ")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine(" DecodeDocument(global::System.ReadOnlySpan<byte> payload)");
        source.AppendLine("    {");
        source.AppendLine("        if (_usesDirectPayloadFormat && global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.IsDirectPayload(payload))");
        source.AppendLine("        {");
        source.AppendLine("            if (!global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.IsBinaryPayload(payload))");
        source.AppendLine("            {");
        source.AppendLine("                return global::System.Text.Json.JsonSerializer.Deserialize(global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.GetJsonUtf8(payload), s_typeInfo)");
        source.AppendLine("                    ?? throw new global::System.InvalidOperationException(\"Generated collection payload deserialized to null.\");");
        source.AppendLine("            }");
        source.AppendLine();
        source.AppendLine("            string json = global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.DecodeJson(payload);");
        source.AppendLine("            return global::System.Text.Json.JsonSerializer.Deserialize(json, s_typeInfo)");
        source.AppendLine("                ?? throw new global::System.InvalidOperationException(\"Generated collection payload deserialized to null.\");");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        global::CSharpDB.Primitives.DbValue[] values = _recordSerializer.Decode(payload);");
        source.AppendLine("        return global::System.Text.Json.JsonSerializer.Deserialize(values[1].AsText, s_typeInfo)");
        source.AppendLine("            ?? throw new global::System.InvalidOperationException(\"Generated collection payload deserialized to null.\");");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    public string DecodeKey(global::System.ReadOnlySpan<byte> payload)");
        source.AppendLine("    {");
        source.AppendLine("        if (_usesDirectPayloadFormat && global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.IsDirectPayload(payload))");
        source.AppendLine("            return global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.DecodeKey(payload);");
        source.AppendLine();
        source.AppendLine("        global::CSharpDB.Primitives.DbValue[] values = _recordSerializer.DecodeUpTo(payload, 0);");
        source.AppendLine("        return values[0].AsText;");
        source.AppendLine("    }");
        source.AppendLine();
        source.Append("    public bool TryDecodeDocumentForKey(global::System.ReadOnlySpan<byte> payload, string expectedKey, out ")
            .Append(target.FullyQualifiedTypeName)
            .AppendLine("? document)");
        source.AppendLine("    {");
        source.AppendLine("        if (!PayloadMatchesKey(payload, expectedKey))");
        source.AppendLine("        {");
        source.AppendLine("            document = default;");
        source.AppendLine("            return false;");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        document = DecodeDocument(payload);");
        source.AppendLine("        return true;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    public bool PayloadMatchesKey(global::System.ReadOnlySpan<byte> payload, string expectedKey)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(expectedKey);");
        source.AppendLine();
        source.AppendLine("        int byteCount = global::System.Text.Encoding.UTF8.GetByteCount(expectedKey);");
        source.AppendLine("        byte[]? rented = null;");
        source.AppendLine("        global::System.Span<byte> utf8 = byteCount <= StackallocKeyThreshold");
        source.AppendLine("            ? stackalloc byte[StackallocKeyThreshold]");
        source.AppendLine("            : (rented = global::System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount));");
        source.AppendLine();
        source.AppendLine("        try");
        source.AppendLine("        {");
        source.AppendLine("            int written = global::System.Text.Encoding.UTF8.GetBytes(expectedKey.AsSpan(), utf8);");
        source.AppendLine("            global::System.ReadOnlySpan<byte> expectedKeyUtf8 = utf8[..written];");
        source.AppendLine();
        source.AppendLine("            if (_usesDirectPayloadFormat && global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.IsDirectPayload(payload))");
        source.AppendLine("                return global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.KeyEquals(payload, expectedKeyUtf8);");
        source.AppendLine();
        source.AppendLine("            if (_recordSerializer.TryColumnTextEquals(payload, 0, expectedKeyUtf8, out bool equals))");
        source.AppendLine("                return equals;");
        source.AppendLine();
        source.AppendLine("            return DecodeKey(payload) == expectedKey;");
        source.AppendLine("        }");
        source.AppendLine("        finally");
        source.AppendLine("        {");
        source.AppendLine("            if (rented is not null)");
        source.AppendLine("            {");
        source.AppendLine("                utf8[..byteCount].Clear();");
        source.AppendLine("                global::System.Buffers.ArrayPool<byte>.Shared.Return(rented);");
        source.AppendLine("            }");
        source.AppendLine("        }");
        source.AppendLine("    }");
        source.AppendLine("}");
        source.AppendLine();

        source.AppendLine("[global::System.CodeDom.Compiler.GeneratedCode(\"CSharpDB.CollectionModelGenerator\", \"3.0.0\")]");
        source.Append("internal static class __CSharpDB_CollectionRegistration_")
            .Append(target.SafeIdentifier)
            .AppendLine();
        source.AppendLine("{");
        source.AppendLine("    [global::System.Runtime.CompilerServices.ModuleInitializer]");
        source.AppendLine("    internal static void Register()");
        source.AppendLine("    {");
        source.Append("        _ = global::CSharpDB.Engine.CollectionModelRegistry.Register<")
            .Append(target.FullyQualifiedTypeName)
            .Append(">(new __CSharpDB_CollectionModel_")
            .Append(target.SafeIdentifier)
            .AppendLine("());");
        source.AppendLine("    }");
        source.AppendLine("}");

        context.AddSource(
            hintName: $"{target.SafeIdentifier}.CollectionModel.g.cs",
            sourceText: SourceText.From(source.ToString(), Encoding.UTF8));
    }

    private readonly struct CollectionGenerationResult
    {
        public CollectionGenerationResult(CollectionModelTarget? target, Diagnostic? diagnostic)
        {
            Target = target;
            Diagnostic = diagnostic;
        }

        public CollectionModelTarget? Target { get; }

        public Diagnostic? Diagnostic { get; }
    }

    private sealed class CollectionModelTarget
    {
        public CollectionModelTarget(
            string? namespaceName,
            string typeName,
            string fullyQualifiedTypeName,
            string jsonContextTypeName,
            string partialTypeKeyword,
            string safeIdentifier,
            ImmutableArray<CollectionFieldSpec> fields)
        {
            NamespaceName = namespaceName;
            TypeName = typeName;
            FullyQualifiedTypeName = fullyQualifiedTypeName;
            JsonContextTypeName = jsonContextTypeName;
            PartialTypeKeyword = partialTypeKeyword;
            SafeIdentifier = safeIdentifier;
            Fields = fields;
        }

        public string? NamespaceName { get; }

        public string TypeName { get; }

        public string FullyQualifiedTypeName { get; }

        public string JsonContextTypeName { get; }

        public string PartialTypeKeyword { get; }

        public string SafeIdentifier { get; }

        public ImmutableArray<CollectionFieldSpec> Fields { get; }
    }

    private readonly struct CollectionFieldSpec
    {
        public CollectionFieldSpec(
            string memberName,
            string escapedMemberName,
            string fieldPath,
            string memberTypeName,
            string dataKindName)
        {
            MemberName = memberName;
            EscapedMemberName = escapedMemberName;
            FieldPath = fieldPath;
            MemberTypeName = memberTypeName;
            DataKindName = dataKindName;
        }

        public string MemberName { get; }

        public string EscapedMemberName { get; }

        public string FieldPath { get; }

        public string MemberTypeName { get; }

        public string DataKindName { get; }
    }
}
