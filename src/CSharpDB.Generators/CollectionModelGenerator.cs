using System;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
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

    private static readonly DiagnosticDescriptor GeneratedFieldNameConflict = new(
        id: "CDBGEN006",
        title: "Generated collection field name conflict",
        messageFormat: "Type '{0}' produces duplicate generated collection member '{1}'. Rename one of the CLR members or use a different collection-model shape.",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor UnsupportedCollectionMember = new(
        id: "CDBGEN007",
        title: "Collection member shape is not supported",
        messageFormat: "Type '{0}' member '{1}' is not supported by generated collection descriptors and will be ignored: {2}",
        category: "CSharpDB.SourceGeneration",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<CollectionGenerationResult> candidates =
            context.SyntaxProvider.ForAttributeWithMetadataName(
                fullyQualifiedMetadataName: CollectionModelAttributeName,
                predicate: static (node, _) => node is TypeDeclarationSyntax,
                transform: static (ctx, _) => InspectTarget(ctx))
            .Where(static result => result.Target is not null || result.Diagnostics.Length > 0);

        context.RegisterSourceOutput(
            candidates,
            static (productionContext, result) =>
            {
                for (int i = 0; i < result.Diagnostics.Length; i++)
                    productionContext.ReportDiagnostic(result.Diagnostics[i]);

                if (result.Target is null)
                {
                    return;
                }

                EmitModel(productionContext, result.Target);
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
                ImmutableArray.Create(Diagnostic.Create(TypeMustBePartial, location, typeSymbol.ToDisplayString())));
        }

        if (typeSymbol.ContainingType is not null)
        {
            return new CollectionGenerationResult(
                null,
                ImmutableArray.Create(Diagnostic.Create(TypeMustBeTopLevel, location, typeSymbol.ToDisplayString())));
        }

        if (typeSymbol.Arity != 0)
        {
            return new CollectionGenerationResult(
                null,
                ImmutableArray.Create(Diagnostic.Create(GenericTypesNotSupported, location, typeSymbol.ToDisplayString())));
        }

        if (typeSymbol.GetMembers("Collection").Any(static member => !member.IsImplicitlyDeclared))
        {
            return new CollectionGenerationResult(
                null,
                ImmutableArray.Create(Diagnostic.Create(ReservedCollectionNameConflict, location, typeSymbol.ToDisplayString())));
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
                ImmutableArray.Create(Diagnostic.Create(JsonContextTypeRequired, location, typeSymbol.ToDisplayString())));
        }

        var fields = ImmutableArray.CreateBuilder<CollectionFieldSpec>();
        var diagnostics = ImmutableArray.CreateBuilder<Diagnostic>();
        CollectFields(
            rootType: typeSymbol,
            currentType: typeSymbol,
            path: ImmutableArray<FieldPathSegment>.Empty,
            recursionStack: ImmutableArray.Create(typeSymbol),
            fields: fields,
            diagnostics: diagnostics);

        if (TryFindFieldNameConflict(fields, out string? conflictingMemberName))
        {
            return new CollectionGenerationResult(
                null,
                ImmutableArray.Create(
                    Diagnostic.Create(
                        GeneratedFieldNameConflict,
                        location,
                        typeSymbol.ToDisplayString(),
                        conflictingMemberName!)));
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
            diagnostics.ToImmutable());
    }

    private static void CollectFields(
        INamedTypeSymbol rootType,
        INamedTypeSymbol currentType,
        ImmutableArray<FieldPathSegment> path,
        ImmutableArray<INamedTypeSymbol> recursionStack,
        ImmutableArray<CollectionFieldSpec>.Builder fields,
        ImmutableArray<Diagnostic>.Builder diagnostics)
    {
        foreach (ISymbol member in currentType.GetMembers().OrderBy(static member => member.Name, StringComparer.Ordinal))
        {
            if (!TryGetCollectionMember(member, out ITypeSymbol? memberType) || memberType is null)
                continue;

            string jsonName = GetJsonPropertyName(member) ?? JsonNamingPolicy.CamelCase.ConvertName(member.Name);
            string memberTypeName = memberType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var pathSegment = new FieldPathSegment(
                member.Name,
                EscapeIdentifier(member.Name),
                jsonName,
                isArray: false,
                canBeNull: CanBeNull(memberType),
                isNullableValueType: IsNullableValueType(memberType),
                memberTypeName,
                elementTypeName: null,
                canElementBeNull: false,
                isElementNullableValueType: false);

            if (TryCreateLeafField(rootType, memberType, path.Add(pathSegment), out CollectionFieldSpec leafField))
            {
                fields.Add(leafField);
                continue;
            }

            if (TryGetNavigableComplexType(memberType, out INamedTypeSymbol? nestedType) &&
                nestedType is not null &&
                !ContainsType(recursionStack, nestedType))
            {
                CollectFields(
                    rootType,
                    nestedType,
                    path.Add(pathSegment),
                    recursionStack.Add(nestedType),
                    fields,
                    diagnostics);

                continue;
            }

            if (TryGetNavigableComplexType(memberType, out nestedType) &&
                nestedType is not null &&
                ContainsType(recursionStack, nestedType))
            {
                diagnostics.Add(CreateUnsupportedMemberDiagnostic(rootType, member, "recursive object graphs are not supported"));
                continue;
            }

            if (TryGetCollectionElementType(memberType, out ITypeSymbol? elementType) &&
                elementType is not null &&
                TryGetNavigableComplexType(elementType, out INamedTypeSymbol? collectionElementType) &&
                collectionElementType is not null &&
                !ContainsType(recursionStack, collectionElementType))
            {
                var collectionSegment = new FieldPathSegment(
                    member.Name,
                    EscapeIdentifier(member.Name),
                    jsonName,
                    isArray: true,
                    canBeNull: CanBeNull(memberType),
                    isNullableValueType: IsNullableValueType(memberType),
                    memberTypeName,
                    elementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    canElementBeNull: CanBeNull(elementType),
                    isElementNullableValueType: IsNullableValueType(elementType));

                CollectFields(
                    rootType,
                    collectionElementType,
                    path.Add(collectionSegment),
                    recursionStack.Add(collectionElementType),
                    fields,
                    diagnostics);

                continue;
            }

            if (TryGetCollectionElementType(memberType, out elementType) &&
                elementType is not null &&
                TryGetNavigableComplexType(elementType, out collectionElementType) &&
                collectionElementType is not null &&
                ContainsType(recursionStack, collectionElementType))
            {
                diagnostics.Add(CreateUnsupportedMemberDiagnostic(rootType, member, "recursive collection element graphs are not supported"));
                continue;
            }

            diagnostics.Add(CreateUnsupportedMemberDiagnostic(rootType, member, DescribeUnsupportedMember(memberType)));
        }
    }

    private static Diagnostic CreateUnsupportedMemberDiagnostic(
        INamedTypeSymbol rootType,
        ISymbol member,
        string reason)
        => Diagnostic.Create(
            UnsupportedCollectionMember,
            member.Locations.FirstOrDefault(),
            rootType.ToDisplayString(),
            member.Name,
            reason);

    private static string DescribeUnsupportedMember(ITypeSymbol memberType)
    {
        if (TryGetCollectionElementType(memberType, out ITypeSymbol? elementType) && elementType is not null)
        {
            return $"collection element type '{elementType.ToDisplayString()}' does not map to a supported scalar or nested object path";
        }

        return $"member type '{memberType.ToDisplayString()}' is not supported; generated fields currently cover string, Guid, DateOnly, TimeOnly, enums, integer types, scalar collections, and nested object paths";
    }

    private static bool TryCreateLeafField(
        INamedTypeSymbol rootType,
        ITypeSymbol memberType,
        ImmutableArray<FieldPathSegment> path,
        out CollectionFieldSpec field)
    {
        field = default;
        if (!TryClassifyFieldType(memberType, out string descriptorTypeName, out string dataKindName, out bool isMultiValue))
            return false;

        FieldPathSegment lastSegment = path[path.Length - 1];
        if (isMultiValue)
            path = path.SetItem(
                path.Length - 1,
                new FieldPathSegment(
                    lastSegment.ClrName,
                    lastSegment.EscapedClrName,
                    lastSegment.JsonName,
                    isArray: true,
                    lastSegment.CanBeNull,
                    lastSegment.IsNullableValueType,
                    lastSegment.MemberTypeName,
                    lastSegment.ElementTypeName,
                    lastSegment.CanElementBeNull,
                    lastSegment.IsElementNullableValueType));

        string generatedMemberName = string.Join("_", path.Select(static segment => segment.ClrName));
        string escapedGeneratedMemberName = EscapeIdentifier(generatedMemberName);
        string fieldPath = BuildPath(path, useJsonNames: false);
        string payloadFieldPath = BuildPath(path, useJsonNames: true);
        string accessorExpression;
        string? accessorHelperSource = null;

        if (path.Length == 1)
        {
            accessorExpression = "static document => document." + path[0].EscapedClrName;
        }
        else
        {
            string helperMethodName = "__Get_" + generatedMemberName;
            accessorExpression = "static document => " + helperMethodName + "(document)";
            accessorHelperSource = BuildNestedAccessorHelper(rootType, helperMethodName, path);
        }

        field = new CollectionFieldSpec(
            generatedMemberName,
            escapedGeneratedMemberName,
            fieldPath,
            payloadFieldPath,
            descriptorTypeName,
            dataKindName,
            accessorExpression,
            accessorHelperSource);
        return true;
    }

    private static bool TryClassifyFieldType(
        ITypeSymbol type,
        out string descriptorTypeName,
        out string dataKindName,
        out bool isMultiValue)
    {
        isMultiValue = false;
        ITypeSymbol descriptorType = type;
        ITypeSymbol effectiveType = UnwrapNullable(type);
        if (TryGetCollectionElementType(type, out ITypeSymbol? elementType))
        {
            isMultiValue = true;
            descriptorType = elementType!;
            effectiveType = UnwrapNullable(elementType!);
        }

        if (effectiveType.SpecialType == SpecialType.System_String ||
            IsWellKnownType(effectiveType, "System.Guid") ||
            IsWellKnownType(effectiveType, "System.DateOnly") ||
            IsWellKnownType(effectiveType, "System.TimeOnly"))
        {
            descriptorTypeName = descriptorType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
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
            descriptorTypeName = descriptorType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            dataKindName = "Integer";
            return true;
        }

        descriptorTypeName = string.Empty;
        dataKindName = string.Empty;
        return false;
    }

    private static bool TryGetCollectionMember(ISymbol member, out ITypeSymbol? memberType)
    {
        memberType = null;
        if (member.IsStatic || member.DeclaredAccessibility != Accessibility.Public || member.IsImplicitlyDeclared)
            return false;

        if (HasAttribute(member, JsonIgnoreAttributeName))
            return false;

        memberType = member switch
        {
            IPropertySymbol property when !property.IsIndexer &&
                                        property.GetMethod is not null &&
                                        property.GetMethod.DeclaredAccessibility == Accessibility.Public =>
                property.Type,
            IFieldSymbol fieldSymbol => fieldSymbol.Type,
            _ => null,
        };

        return memberType is not null;
    }

    private static bool TryGetNavigableComplexType(ITypeSymbol type, out INamedTypeSymbol? complexType)
    {
        complexType = null;

        if (TryClassifyFieldType(type, out _, out _, out _))
            return false;

        if (TryGetCollectionElementType(type, out _))
            return false;

        ITypeSymbol effectiveType = UnwrapNullable(type);
        if (effectiveType is not INamedTypeSymbol namedType)
            return false;

        if (effectiveType.TypeKind is not TypeKind.Class and not TypeKind.Struct)
            return false;

        if (effectiveType.SpecialType != SpecialType.None)
            return false;

        complexType = namedType;
        return true;
    }

    private static bool ContainsType(ImmutableArray<INamedTypeSymbol> recursionStack, INamedTypeSymbol type)
    {
        for (int i = 0; i < recursionStack.Length; i++)
        {
            if (SymbolEqualityComparer.Default.Equals(recursionStack[i], type))
                return true;
        }

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

    private static string? GetJsonPropertyName(ISymbol symbol)
    {
        foreach (AttributeData attribute in symbol.GetAttributes())
        {
            if (!string.Equals(
                    attribute.AttributeClass?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    "global::" + JsonPropertyNameAttributeName,
                    StringComparison.Ordinal))
            {
                continue;
            }

            if (attribute.ConstructorArguments.Length == 1 &&
                attribute.ConstructorArguments[0].Value is string propertyName &&
                !string.IsNullOrWhiteSpace(propertyName))
            {
                return propertyName;
            }
        }

        return null;
    }

    private static bool TryFindFieldNameConflict(
        ImmutableArray<CollectionFieldSpec>.Builder fields,
        out string? conflictingMemberName)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < fields.Count; i++)
        {
            if (!seen.Add(fields[i].GeneratedMemberName))
            {
                conflictingMemberName = fields[i].GeneratedMemberName;
                return true;
            }
        }

        conflictingMemberName = null;
        return false;
    }

    private static bool CanBeNull(ITypeSymbol type)
        => type.IsReferenceType ||
           (type is INamedTypeSymbol named &&
            named.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

    private static bool IsNullableValueType(ITypeSymbol type)
        => type is INamedTypeSymbol named &&
           named.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;

    private static string BuildPath(ImmutableArray<FieldPathSegment> path, bool useJsonNames)
    {
        var builder = new StringBuilder();
        for (int i = 0; i < path.Length; i++)
        {
            if (i > 0)
                builder.Append('.');

            builder.Append(useJsonNames ? path[i].JsonName : path[i].ClrName);
            if (path[i].IsArray)
                builder.Append("[]");
        }

        return builder.ToString();
    }

    private static string BuildNestedAccessorHelper(
        INamedTypeSymbol rootType,
        string helperMethodName,
        ImmutableArray<FieldPathSegment> path)
    {
        var source = new StringBuilder();
        source.Append("        private static object? ")
            .Append(helperMethodName)
            .Append('(')
            .Append(rootType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat))
            .AppendLine(" document)");
        source.AppendLine("        {");

        bool containsArray = path.Any(static segment => segment.IsArray);
        if (containsArray)
        {
            source.AppendLine("            global::System.Collections.Generic.List<object?>? values = null;");
            EmitArrayTraversal(source, path, "document", 0, "            ");
            source.AppendLine("            return values is null || values.Count == 0 ? null : values;");
            source.AppendLine("        }");
            return source.ToString();
        }

        for (int i = 0; i < path.Length - 1; i++)
        {
            string current = i == 0 ? "document" : "value" + (i - 1).ToString();
            string next = "value" + i.ToString();
            source.Append("            var ")
                .Append(next)
                .Append(" = ")
                .Append(current)
                .Append('.')
                .Append(path[i].EscapedClrName)
                .AppendLine(";");

            if (path[i].CanBeNull)
            {
                source.Append("            if (")
                    .Append(next)
                    .AppendLine(" is null)");
                source.AppendLine("                return null;");
            }
        }

        string leafOwner = path.Length == 1 ? "document" : "value" + (path.Length - 2).ToString();
        source.Append("            return ")
            .Append(leafOwner)
            .Append('.')
            .Append(path[path.Length - 1].EscapedClrName)
            .AppendLine(";");
        source.AppendLine("        }");
        return source.ToString();
    }

    private static void EmitArrayTraversal(
        StringBuilder source,
        ImmutableArray<FieldPathSegment> path,
        string currentExpression,
        int pathIndex,
        string indent)
    {
        FieldPathSegment segment = path[pathIndex];
        string valueVariable = "value" + pathIndex.ToString();
        source.Append(indent)
            .Append("var ")
            .Append(valueVariable)
            .Append(" = ")
            .Append(currentExpression)
            .Append('.')
            .Append(segment.EscapedClrName)
            .AppendLine(";");

        if (segment.IsArray)
        {
            source.Append(indent)
                .Append("if (")
                .Append(valueVariable)
                .AppendLine(" is not null)");
            source.Append(indent).AppendLine("{");

            string itemVariable = "item" + pathIndex.ToString();
            source.Append(indent)
                .Append("    foreach (var ")
                .Append(itemVariable)
                .Append(" in ")
                .Append(valueVariable)
                .AppendLine(")");
            source.Append(indent).AppendLine("    {");

            string itemExpression = itemVariable;
            if (segment.CanElementBeNull)
            {
                source.Append(indent)
                    .Append("        if (")
                    .Append(itemVariable)
                    .AppendLine(" is null)");
                source.Append(indent).AppendLine("            continue;");
                if (segment.IsElementNullableValueType)
                    itemExpression += ".Value";
            }

            if (pathIndex == path.Length - 1)
            {
                source.Append(indent).AppendLine("        values ??= new global::System.Collections.Generic.List<object?>();");
                source.Append(indent)
                    .Append("        values.Add(")
                    .Append(itemExpression)
                    .AppendLine(");");
            }
            else
            {
                EmitArrayTraversal(source, path, itemExpression, pathIndex + 1, indent + "        ");
            }

            source.Append(indent).AppendLine("    }");
            source.Append(indent).AppendLine("}");
            return;
        }

        string nextExpression = valueVariable;
        if (segment.CanBeNull)
        {
            source.Append(indent)
                .Append("if (")
                .Append(valueVariable)
                .AppendLine(" is not null)");
            source.Append(indent).AppendLine("{");
            if (segment.IsNullableValueType)
                nextExpression += ".Value";
            indent += "    ";
        }

        if (pathIndex == path.Length - 1)
        {
            source.Append(indent).AppendLine("values ??= new global::System.Collections.Generic.List<object?>();");
            source.Append(indent)
                .Append("values.Add(")
                .Append(nextExpression)
                .AppendLine(");");
        }
        else
        {
            EmitArrayTraversal(source, path, nextExpression, pathIndex + 1, indent);
        }

        if (segment.CanBeNull)
        {
            indent = indent.Substring(0, indent.Length - 4);
            source.Append(indent).AppendLine("}");
        }
    }

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
                .Append(", ")
                .Append(field.AccessorExpression)
                .Append(", global::CSharpDB.Engine.CollectionIndexDataKind.")
                .Append(field.DataKindName)
                .Append(", ")
                .Append(SymbolDisplay.FormatLiteral(field.PayloadFieldPath, quote: true));
            source.AppendLine(");");
            source.AppendLine();
        }

        foreach (CollectionFieldSpec field in target.Fields)
        {
            if (field.AccessorHelperSource is null)
                continue;

            source.Append(field.AccessorHelperSource);
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
        public CollectionGenerationResult(CollectionModelTarget? target, ImmutableArray<Diagnostic> diagnostics)
        {
            Target = target;
            Diagnostics = diagnostics;
        }

        public CollectionModelTarget? Target { get; }

        public ImmutableArray<Diagnostic> Diagnostics { get; }
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
            string generatedMemberName,
            string escapedMemberName,
            string fieldPath,
            string payloadFieldPath,
            string memberTypeName,
            string dataKindName,
            string accessorExpression,
            string? accessorHelperSource)
        {
            GeneratedMemberName = generatedMemberName;
            EscapedMemberName = escapedMemberName;
            FieldPath = fieldPath;
            PayloadFieldPath = payloadFieldPath;
            MemberTypeName = memberTypeName;
            DataKindName = dataKindName;
            AccessorExpression = accessorExpression;
            AccessorHelperSource = accessorHelperSource;
        }

        public string GeneratedMemberName { get; }

        public string EscapedMemberName { get; }

        public string FieldPath { get; }

        public string PayloadFieldPath { get; }

        public string MemberTypeName { get; }

        public string DataKindName { get; }

        public string AccessorExpression { get; }

        public string? AccessorHelperSource { get; }
    }

    private readonly struct FieldPathSegment
    {
        public FieldPathSegment(
            string clrName,
            string escapedClrName,
            string jsonName,
            bool isArray,
            bool canBeNull,
            bool isNullableValueType,
            string memberTypeName,
            string? elementTypeName,
            bool canElementBeNull,
            bool isElementNullableValueType)
        {
            ClrName = clrName;
            EscapedClrName = escapedClrName;
            JsonName = jsonName;
            IsArray = isArray;
            CanBeNull = canBeNull;
            IsNullableValueType = isNullableValueType;
            MemberTypeName = memberTypeName;
            ElementTypeName = elementTypeName;
            CanElementBeNull = canElementBeNull;
            IsElementNullableValueType = isElementNullableValueType;
        }

        public string ClrName { get; }

        public string EscapedClrName { get; }

        public string JsonName { get; }

        public bool IsArray { get; }

        public bool CanBeNull { get; }

        public bool IsNullableValueType { get; }

        public string MemberTypeName { get; }

        public string? ElementTypeName { get; }

        public bool CanElementBeNull { get; }

        public bool IsElementNullableValueType { get; }
    }
}
