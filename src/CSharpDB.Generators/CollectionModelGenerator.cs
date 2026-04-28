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
                fields.ToImmutable(),
                TryCreateBinaryTypeSpec(typeSymbol, ImmutableArray<INamedTypeSymbol>.Empty)),
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

    private static BinaryTypeSpec? TryCreateBinaryTypeSpec(
        INamedTypeSymbol type,
        ImmutableArray<INamedTypeSymbol> recursionStack)
    {
        if (ContainsType(recursionStack, type))
            return null;

        var memberCandidates = ImmutableArray.CreateBuilder<BinaryMemberCandidate>();
        ImmutableArray<INamedTypeSymbol> nextStack = recursionStack.Add(type);
        foreach (ISymbol member in type.GetMembers().OrderBy(static member => member.Name, StringComparer.Ordinal))
        {
            if (!TryGetCollectionMember(member, out ITypeSymbol? memberType) || memberType is null)
                continue;

            if (!TryCreateBinaryValueSpec(memberType, nextStack, out BinaryValueSpec valueSpec))
                return null;

            memberCandidates.Add(new BinaryMemberCandidate(
                member,
                memberType,
                valueSpec));
        }

        ImmutableArray<BinaryMemberCandidate> candidateMembers = memberCandidates.ToImmutable();
        if (!TryCreateBinaryConstructorSpec(type, candidateMembers, out BinaryConstructorSpec constructor))
            return null;

        candidateMembers = OrderBinaryMembersByConstructor(candidateMembers, constructor, out constructor);

        var members = ImmutableArray.CreateBuilder<BinaryMemberSpec>(candidateMembers.Length);
        for (int i = 0; i < candidateMembers.Length; i++)
        {
            BinaryMemberCandidate candidate = candidateMembers[i];
            ISymbol member = candidate.Member;
            members.Add(new BinaryMemberSpec(
                member.Name,
                EscapeIdentifier(member.Name),
                GetJsonPropertyName(member) ?? JsonNamingPolicy.CamelCase.ConvertName(member.Name),
                candidate.Value));
        }

        return new BinaryTypeSpec(
            type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            MakeSafeIdentifier(type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)),
            members.ToImmutable(),
            constructor);
    }

    private static bool TryCreateBinaryConstructorSpec(
        INamedTypeSymbol type,
        ImmutableArray<BinaryMemberCandidate> members,
        out BinaryConstructorSpec constructor)
    {
        foreach (IMethodSymbol candidate in type.InstanceConstructors)
        {
            if (!CanGeneratedCodeCall(candidate) ||
                candidate.Parameters.Length != members.Length)
            {
                continue;
            }

            var memberIndexes = ImmutableArray.CreateBuilder<int>(candidate.Parameters.Length);
            var usedMemberIndexes = new HashSet<int>();
            bool matches = true;
            foreach (IParameterSymbol parameter in candidate.Parameters)
            {
                int memberIndex = FindConstructorMemberIndex(parameter, members, usedMemberIndexes);
                if (memberIndex < 0)
                {
                    matches = false;
                    break;
                }

                usedMemberIndexes.Add(memberIndex);
                memberIndexes.Add(memberIndex);
            }

            if (!matches)
                continue;

            constructor = new BinaryConstructorSpec(memberIndexes.ToImmutable());
            return true;
        }

        constructor = default;
        return false;
    }

    private static ImmutableArray<BinaryMemberCandidate> OrderBinaryMembersByConstructor(
        ImmutableArray<BinaryMemberCandidate> members,
        BinaryConstructorSpec constructor,
        out BinaryConstructorSpec remappedConstructor)
    {
        var orderedMembers = ImmutableArray.CreateBuilder<BinaryMemberCandidate>(members.Length);
        var remappedParameterMemberIndexes = ImmutableArray.CreateBuilder<int>(constructor.ParameterMemberIndexes.Length);
        var usedMemberIndexes = new HashSet<int>();

        for (int i = 0; i < constructor.ParameterMemberIndexes.Length; i++)
        {
            int memberIndex = constructor.ParameterMemberIndexes[i];
            if ((uint)memberIndex >= (uint)members.Length || !usedMemberIndexes.Add(memberIndex))
                throw new InvalidOperationException("Generated binary constructor member indexes are invalid.");

            remappedParameterMemberIndexes.Add(orderedMembers.Count);
            orderedMembers.Add(members[memberIndex]);
        }

        for (int i = 0; i < members.Length; i++)
        {
            if (usedMemberIndexes.Add(i))
                orderedMembers.Add(members[i]);
        }

        remappedConstructor = new BinaryConstructorSpec(remappedParameterMemberIndexes.ToImmutable());
        return orderedMembers.ToImmutable();
    }

    private static bool CanGeneratedCodeCall(IMethodSymbol constructor)
        => constructor.DeclaredAccessibility is
            Accessibility.Public or
            Accessibility.Internal or
            Accessibility.ProtectedOrInternal;

    private static int FindConstructorMemberIndex(
        IParameterSymbol parameter,
        ImmutableArray<BinaryMemberCandidate> members,
        HashSet<int> usedMemberIndexes)
    {
        for (int i = 0; i < members.Length; i++)
        {
            if (usedMemberIndexes.Contains(i))
                continue;

            BinaryMemberCandidate member = members[i];
            if (!string.Equals(parameter.Name, member.Member.Name, StringComparison.OrdinalIgnoreCase))
                continue;

            if (!SymbolEqualityComparer.Default.Equals(parameter.Type, member.Type))
                continue;

            return i;
        }

        return -1;
    }

    private static bool TryCreateBinaryValueSpec(
        ITypeSymbol type,
        ImmutableArray<INamedTypeSymbol> recursionStack,
        out BinaryValueSpec valueSpec)
    {
        if (TryGetBinaryCollectionElementType(type, out ITypeSymbol? elementType) && elementType is not null)
        {
            if (TryGetCollectionElementType(elementType, out _))
            {
                valueSpec = null!;
                return false;
            }

            if (!TryCreateBinaryElementValueSpec(elementType, recursionStack, out BinaryValueSpec elementSpec))
            {
                valueSpec = null!;
                return false;
            }

            valueSpec = BinaryValueSpec.ForArray(
                type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                type is IArrayTypeSymbol,
                CanBeNull(type),
                elementSpec);
            return true;
        }

        return TryCreateBinaryElementValueSpec(type, recursionStack, out valueSpec);
    }

    private static bool TryCreateBinaryElementValueSpec(
        ITypeSymbol type,
        ImmutableArray<INamedTypeSymbol> recursionStack,
        out BinaryValueSpec valueSpec)
    {
        ITypeSymbol effectiveType = UnwrapNullable(type);
        if (TryGetBinaryScalarKind(effectiveType, out string valueKindName))
        {
            valueSpec = BinaryValueSpec.ForScalar(
                type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                effectiveType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                valueKindName,
                CanBeNull(type),
                IsNullableValueType(type));
            return true;
        }

        if (TryGetNavigableComplexType(type, out INamedTypeSymbol? nestedType) &&
            nestedType is not null &&
            !ContainsType(recursionStack, nestedType))
        {
            BinaryTypeSpec? nestedSpec = TryCreateBinaryTypeSpec(nestedType, recursionStack);
            if (nestedSpec is not null)
            {
                valueSpec = BinaryValueSpec.ForObject(
                    type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    effectiveType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    CanBeNull(type),
                    IsNullableValueType(type),
                    nestedSpec);
                return true;
            }
        }

        valueSpec = null!;
        return false;
    }

    private static bool TryGetBinaryScalarKind(ITypeSymbol type, out string valueKindName)
    {
        if (type.SpecialType == SpecialType.System_String)
        {
            valueKindName = "String";
            return true;
        }

        if (IsWellKnownType(type, "System.Guid"))
        {
            valueKindName = "Guid";
            return true;
        }

        if (IsWellKnownType(type, "System.DateOnly"))
        {
            valueKindName = "DateOnly";
            return true;
        }

        if (IsWellKnownType(type, "System.TimeOnly"))
        {
            valueKindName = "TimeOnly";
            return true;
        }

        if (type.TypeKind == TypeKind.Enum)
        {
            valueKindName = "Enum";
            return true;
        }

        valueKindName = type.SpecialType switch
        {
            SpecialType.System_Boolean => "Boolean",
            SpecialType.System_Byte => "Byte",
            SpecialType.System_SByte => "SByte",
            SpecialType.System_Int16 => "Int16",
            SpecialType.System_UInt16 => "UInt16",
            SpecialType.System_Int32 => "Int32",
            SpecialType.System_UInt32 => "UInt32",
            SpecialType.System_Int64 => "Int64",
            SpecialType.System_Single => "Single",
            SpecialType.System_Double => "Double",
            SpecialType.System_Decimal => "Decimal",
            _ => string.Empty,
        };

        return valueKindName.Length > 0;
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

    private static bool TryGetBinaryCollectionElementType(ITypeSymbol type, out ITypeSymbol? elementType)
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
            IsCountedEnumerableLike(namedType.ConstructedFrom))
        {
            elementType = namedType.TypeArguments[0];
            return true;
        }

        foreach (INamedTypeSymbol interfaceType in namedType.AllInterfaces)
        {
            if (interfaceType.IsGenericType &&
                interfaceType.TypeArguments.Length == 1 &&
                IsCountedEnumerableLike(interfaceType.ConstructedFrom))
            {
                elementType = interfaceType.TypeArguments[0];
                return true;
            }
        }

        return false;
    }

    private static bool IsCountedEnumerableLike(INamedTypeSymbol constructedType)
    {
        string displayName = constructedType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return displayName is
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

    private static string CreateBinaryPayloadFieldMethodName(CollectionFieldSpec field)
        => "TryReadBinaryPayloadField_" + MakeSafeIdentifier(field.GeneratedMemberName);

    private static string CreateBinaryPayloadValueReaderName(CollectionFieldSpec field)
        => "TryReadPayloadValue_" + MakeSafeIdentifier(field.GeneratedMemberName);

    private static string CreateBinaryPayloadIntegerReaderName(CollectionFieldSpec field)
        => "TryReadPayloadInteger_" + MakeSafeIdentifier(field.GeneratedMemberName);

    private static string CreateBinaryPayloadTextReaderName(CollectionFieldSpec field)
        => "TryReadPayloadText_" + MakeSafeIdentifier(field.GeneratedMemberName);

    private static string CreateBinaryPayloadTextUtf8ReaderName(CollectionFieldSpec field)
        => "TryReadPayloadTextUtf8_" + MakeSafeIdentifier(field.GeneratedMemberName);

    private static bool TryCreateBinaryFieldReaderSpec(
        BinaryTypeSpec root,
        CollectionFieldSpec field,
        out BinaryFieldReaderSpec reader)
    {
        reader = default;
        if (field.PayloadFieldPath.IndexOf('[') >= 0 ||
            field.PayloadFieldPath.IndexOf(']') >= 0)
        {
            return false;
        }

        string[] pathSegments = field.PayloadFieldPath.Split('.');
        if (pathSegments.Length == 0)
            return false;

        var memberIndexes = ImmutableArray.CreateBuilder<int>(pathSegments.Length);
        BinaryTypeSpec currentType = root;
        BinaryValueSpec? value = null;
        for (int i = 0; i < pathSegments.Length; i++)
        {
            if (!TryFindBinaryMember(currentType, pathSegments[i], out int memberIndex))
                return false;

            memberIndexes.Add(memberIndex);
            value = currentType.Members[memberIndex].Value;
            if (i == pathSegments.Length - 1)
                break;

            if (value.ObjectType is null)
                return false;

            currentType = value.ObjectType;
        }

        if (value is null || value.IsArray || value.ObjectType is not null)
            return false;

        if (!IsSupportedBinaryPayloadFieldReader(value, field.DataKindName))
            return false;

        reader = new BinaryFieldReaderSpec(memberIndexes.ToImmutable(), value, field.DataKindName);
        return true;
    }

    private static bool TryFindBinaryMember(BinaryTypeSpec type, string jsonName, out int memberIndex)
    {
        for (int i = 0; i < type.Members.Length; i++)
        {
            if (string.Equals(type.Members[i].JsonName, jsonName, StringComparison.Ordinal))
            {
                memberIndex = i;
                return true;
            }
        }

        memberIndex = -1;
        return false;
    }

    private static bool IsSupportedBinaryPayloadFieldReader(BinaryValueSpec value, string dataKindName)
        => dataKindName switch
        {
            "Integer" => value.ValueKindName is "Byte" or "SByte" or "Int16" or "UInt16" or "Int32" or "UInt32" or "Int64" or "Enum",
            "Text" => value.ValueKindName is "String" or "Guid" or "DateOnly" or "TimeOnly",
            _ => false,
        };

    private static bool CanUseBinaryRecordFormat(BinaryTypeSpec type)
    {
        for (int i = 0; i < type.Members.Length; i++)
        {
            if (ContainsBinaryArray(type.Members[i].Value))
                return false;
        }

        return true;
    }

    private static bool ContainsBinaryArray(BinaryValueSpec value)
    {
        if (value.IsArray)
            return true;

        if (value.ObjectType is not null && !CanUseBinaryRecordFormat(value.ObjectType))
            return true;

        return value.Element is not null && ContainsBinaryArray(value.Element);
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
            BinaryFieldReaderSpec binaryFieldReader = default;
            bool hasBinaryPayloadReader = target.BinaryModel is not null &&
                                          TryCreateBinaryFieldReaderSpec(target.BinaryModel, field, out binaryFieldReader);
            string codecTypeName = "__CSharpDB_CollectionCodec_" + target.SafeIdentifier;
            string? fieldTypeName = hasBinaryPayloadReader
                ? "__CSharpDB_CollectionField_" + MakeSafeIdentifier(field.GeneratedMemberName)
                : null;

            if (fieldTypeName is not null)
                EmitGeneratedCollectionFieldType(source, target, field, binaryFieldReader, codecTypeName, fieldTypeName);

            source.Append("        public static global::CSharpDB.Engine.CollectionField<")
                .Append(target.FullyQualifiedTypeName)
                .Append(", ")
                .Append(field.MemberTypeName)
                .Append("> ")
                .Append(field.EscapedMemberName)
                .AppendLine(" { get; } =");
            if (fieldTypeName is not null)
            {
                source.Append("            new ")
                    .Append(fieldTypeName)
                    .AppendLine("();");
                source.AppendLine();
                continue;
            }

            source.Append("            new(")
                .Append(SymbolDisplay.FormatLiteral(field.FieldPath, quote: true))
                .Append(", ")
                .Append(field.AccessorExpression)
                .Append(", global::CSharpDB.Engine.CollectionIndexDataKind.")
                .Append(field.DataKindName)
                .Append(", ")
                .Append(SymbolDisplay.FormatLiteral(field.PayloadFieldPath, quote: true));
            if (hasBinaryPayloadReader)
            {
                source.Append(", ")
                    .Append(codecTypeName)
                    .Append('.')
                    .Append(CreateBinaryPayloadValueReaderName(field))
                    .Append(", ");
                if (binaryFieldReader.DataKindName == "Integer")
                {
                    source.Append(codecTypeName)
                        .Append('.')
                        .Append(CreateBinaryPayloadIntegerReaderName(field));
                }
                else
                {
                    source.Append("null");
                }

                source.Append(", ");
                if (binaryFieldReader.DataKindName == "Text")
                {
                    source.Append(codecTypeName)
                        .Append('.')
                        .Append(CreateBinaryPayloadTextReaderName(field));
                }
                else
                {
                    source.Append("null");
                }
            }

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
        if (target.BinaryModel is not null)
        {
            source.AppendLine("    private const byte RecordFormatMarker = 0xD0;");
            source.AppendLine("    private const byte RecordFormatMagic = 0xF0;");
            source.AppendLine("    private const byte RecordFormatVersion = 0x01;");
            source.AppendLine("    private const byte NullTag = 0;");
            source.AppendLine("    private const byte StringTag = 1;");
            source.AppendLine("    private const byte IntegerTag = 2;");
            source.AppendLine("    private const byte FalseTag = 3;");
            source.AppendLine("    private const byte TrueTag = 4;");
            source.AppendLine("    private const byte DoubleTag = 5;");
            source.AppendLine("    private const byte DecimalTag = 6;");
            source.AppendLine("    private const byte ObjectTag = 7;");
            source.AppendLine("    private const byte ArrayTag = 8;");
        }

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
        if (target.BinaryModel is not null)
        {
            source.AppendLine("            int documentPayloadLength = GetBinaryDocumentSize(document);");
            source.AppendLine("            byte[] payload = global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.EncodeBinary(key, documentPayloadLength, out int documentPayloadStart);");
            source.AppendLine("            WriteBinaryDocument(payload.AsSpan(documentPayloadStart, documentPayloadLength), document);");
            source.AppendLine("            return payload;");
        }
        else
        {
            source.AppendLine("            byte[] jsonUtf8 = global::System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(document, s_typeInfo);");
            source.AppendLine("            return global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.Encode(key, jsonUtf8);");
        }

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
        if (target.BinaryModel is not null)
        {
            source.AppendLine("        if (_usesDirectPayloadFormat && global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.TryGetBinaryDocumentPayload(payload, out global::System.ReadOnlySpan<byte> binaryDocument))");
            source.AppendLine("            return DecodeBinaryDocument(binaryDocument);");
            source.AppendLine();
        }

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
        source.AppendLine("        if (_usesDirectPayloadFormat && global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.TryDecodeDirectPayloadKey(payload, out string key))");
        source.AppendLine("            return key;");
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
        source.AppendLine("        if (_usesDirectPayloadFormat && global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.TryDirectPayloadKeyEquals(payload, expectedKey, out bool directEquals))");
        source.AppendLine("            return directEquals;");
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
        if (target.BinaryModel is not null)
        {
            source.AppendLine();
            EmitBinaryCodecMembers(source, target.BinaryModel, target.Fields);
        }

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

    private static void EmitGeneratedCollectionFieldType(
        StringBuilder source,
        CollectionModelTarget target,
        CollectionFieldSpec field,
        BinaryFieldReaderSpec binaryFieldReader,
        string codecTypeName,
        string fieldTypeName)
    {
        source.Append("        private sealed class ")
            .Append(fieldTypeName)
            .Append(" : global::CSharpDB.Engine.CollectionField<")
            .Append(target.FullyQualifiedTypeName)
            .Append(", ")
            .Append(field.MemberTypeName)
            .AppendLine(">");
        source.AppendLine("        {");
        source.Append("            public ")
            .Append(fieldTypeName)
            .AppendLine("()");
        source.Append("                : base(")
            .Append(SymbolDisplay.FormatLiteral(field.FieldPath, quote: true))
            .Append(", ")
            .Append(field.AccessorExpression)
            .Append(", global::CSharpDB.Engine.CollectionIndexDataKind.")
            .Append(field.DataKindName)
            .Append(", ")
            .Append(SymbolDisplay.FormatLiteral(field.PayloadFieldPath, quote: true))
            .AppendLine(")");
        source.AppendLine("            {");
        source.AppendLine("            }");
        source.AppendLine();

        source.AppendLine("            public override bool TryReadPayloadValue(global::System.ReadOnlySpan<byte> payload, out global::CSharpDB.Primitives.DbValue value)");
        source.AppendLine("            {");
        source.Append("                if (")
            .Append(codecTypeName)
            .Append('.')
            .Append(CreateBinaryPayloadValueReaderName(field))
            .AppendLine("(payload, out value))");
        source.AppendLine("                    return true;");
        source.AppendLine();
        source.AppendLine("                return base.TryReadPayloadValue(payload, out value);");
        source.AppendLine("            }");

        if (binaryFieldReader.DataKindName == "Integer")
        {
            source.AppendLine();
            source.AppendLine("            public override bool TryReadPayloadInt64(global::System.ReadOnlySpan<byte> payload, out long value)");
            source.AppendLine("            {");
            source.Append("                if (")
                .Append(codecTypeName)
                .Append('.')
                .Append(CreateBinaryPayloadIntegerReaderName(field))
                .AppendLine("(payload, out value))");
            source.AppendLine("                    return true;");
            source.AppendLine();
            source.AppendLine("                return base.TryReadPayloadInt64(payload, out value);");
            source.AppendLine("            }");
        }
        else if (binaryFieldReader.DataKindName == "Text")
        {
            source.AppendLine();
            source.AppendLine("            public override bool TryReadPayloadString(global::System.ReadOnlySpan<byte> payload, out string? value)");
            source.AppendLine("            {");
            source.Append("                if (")
                .Append(codecTypeName)
                .Append('.')
                .Append(CreateBinaryPayloadTextReaderName(field))
                .AppendLine("(payload, out value))");
            source.AppendLine("                    return true;");
            source.AppendLine();
            source.AppendLine("                return base.TryReadPayloadString(payload, out value);");
            source.AppendLine("            }");
            source.AppendLine();
            source.AppendLine("            public override bool TryReadPayloadStringUtf8(global::System.ReadOnlySpan<byte> payload, out global::System.ReadOnlySpan<byte> value)");
            source.AppendLine("            {");
            source.Append("                if (")
                .Append(codecTypeName)
                .Append('.')
                .Append(CreateBinaryPayloadTextUtf8ReaderName(field))
                .AppendLine("(payload, out value))");
            source.AppendLine("                    return true;");
            source.AppendLine();
            source.AppendLine("                return base.TryReadPayloadStringUtf8(payload, out value);");
            source.AppendLine("            }");
        }

        source.AppendLine("        }");
        source.AppendLine();
    }

    private static void EmitBinaryCodecMembers(
        StringBuilder source,
        BinaryTypeSpec root,
        ImmutableArray<CollectionFieldSpec> fields)
    {
        bool useRecordFormat = CanUseBinaryRecordFormat(root);

        source.Append("    private static int GetBinaryDocumentSize(")
            .Append(root.TypeName)
            .AppendLine(" document)");
        source.Append("        => ");
        if (useRecordFormat)
            source.Append("3 + GetBinaryRecordSize_");
        else
            source.Append("GetBinarySize_");

        source.Append(root.SafeIdentifier).AppendLine("(document);");
        source.AppendLine();

        source.Append("    private static void WriteBinaryDocument(global::System.Span<byte> destination, ")
            .Append(root.TypeName)
            .AppendLine(" document)");
        source.AppendLine("    {");
        source.AppendLine("        int position = 0;");
        if (useRecordFormat)
        {
            source.AppendLine("        WriteByte(destination, ref position, RecordFormatMarker);");
            source.AppendLine("        WriteByte(destination, ref position, RecordFormatMagic);");
            source.AppendLine("        WriteByte(destination, ref position, RecordFormatVersion);");
            source.Append("        WriteBinaryRecord_").Append(root.SafeIdentifier).AppendLine("(destination, ref position, document);");
        }
        else
        {
            source.Append("        WriteBinary_").Append(root.SafeIdentifier).AppendLine("(destination, ref position, document);");
        }

        source.AppendLine("        if (position != destination.Length)");
        source.AppendLine("            throw new global::System.InvalidOperationException(\"Generated binary collection payload size mismatch.\");");
        source.AppendLine("    }");
        source.AppendLine();

        source.Append("    private static ")
            .Append(root.TypeName)
            .AppendLine(" DecodeBinaryDocument(global::System.ReadOnlySpan<byte> payload)");
        source.AppendLine("    {");
        source.AppendLine("        int position = 0;");
        if (useRecordFormat)
        {
            source.AppendLine("        if (IsBinaryRecordPayload(payload))");
            source.AppendLine("        {");
            source.AppendLine("            position = 3;");
            source.Append("            ")
                .Append(root.TypeName)
                .Append(" recordDocument = ReadBinaryRecord_")
                .Append(root.SafeIdentifier)
                .AppendLine("(payload, ref position);");
            source.AppendLine("            if (position != payload.Length)");
            source.AppendLine("                throw new global::CSharpDB.Primitives.CSharpDbException(global::CSharpDB.Primitives.ErrorCode.CorruptDatabase, \"Invalid generated binary collection payload length.\");");
            source.AppendLine();
            source.AppendLine("            return recordDocument;");
            source.AppendLine("        }");
            source.AppendLine();
        }

        source.Append("        ")
            .Append(root.TypeName)
            .Append(" document = ReadBinary_")
            .Append(root.SafeIdentifier)
            .AppendLine("(payload, ref position);");
        source.AppendLine("        if (position != payload.Length)");
        source.AppendLine("            throw new global::CSharpDB.Primitives.CSharpDbException(global::CSharpDB.Primitives.ErrorCode.CorruptDatabase, \"Invalid generated binary collection payload length.\");");
        source.AppendLine();
        source.AppendLine("        return document;");
        source.AppendLine("    }");
        source.AppendLine();

        var emittedSizeTypes = new HashSet<string>(StringComparer.Ordinal);
        EmitBinaryTypeSizer(source, root, emittedSizeTypes);
        var emittedWriterTypes = new HashSet<string>(StringComparer.Ordinal);
        EmitBinaryTypeWriter(source, root, emittedWriterTypes);
        var emittedReaderTypes = new HashSet<string>(StringComparer.Ordinal);
        EmitBinaryTypeReader(source, root, emittedReaderTypes);
        if (useRecordFormat)
        {
            var emittedRecordSizeTypes = new HashSet<string>(StringComparer.Ordinal);
            EmitBinaryRecordTypeSizer(source, root, emittedRecordSizeTypes);
            var emittedRecordWriterTypes = new HashSet<string>(StringComparer.Ordinal);
            EmitBinaryRecordTypeWriter(source, root, emittedRecordWriterTypes);
            var emittedRecordReaderTypes = new HashSet<string>(StringComparer.Ordinal);
            EmitBinaryRecordTypeReader(source, root, emittedRecordReaderTypes);
            var emittedRecordSkipperTypes = new HashSet<string>(StringComparer.Ordinal);
            EmitBinaryRecordTypeSkipper(source, root, emittedRecordSkipperTypes);
        }

        EmitBinaryPayloadFieldReaders(source, root, fields);
        EmitBinaryPrimitiveHelpers(source);
    }

    private static void EmitBinaryTypeSizer(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        source.Append("    private static int GetBinarySize_")
            .Append(type.SafeIdentifier)
            .Append('(')
            .Append(type.TypeName)
            .AppendLine(" value)");
        source.AppendLine("    {");
        source.Append("        int size = GetVarintSize((ulong)")
            .Append(type.Members.Length.ToString(System.Globalization.CultureInfo.InvariantCulture))
            .AppendLine(");");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            string valueVariable = "value" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            source.Append("        size += GetLengthPrefixedUtf8Size(")
                .Append(SymbolDisplay.FormatLiteral(member.JsonName, quote: true))
                .AppendLine("u8);");
            source.Append("        var ")
                .Append(valueVariable)
                .Append(" = value.")
                .Append(member.EscapedClrName)
                .AppendLine(";");
            EmitGetBinaryValueSize(source, member.Value, valueVariable, "        ", i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        source.AppendLine("        return size;");
        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryTypeSizers(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryTypeSizers(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryTypeSizer(source, value.ObjectType, emittedTypes);

        if (value.Element is not null)
            EmitNestedBinaryTypeSizers(source, value.Element, emittedTypes);
    }

    private static void EmitBinaryTypeWriter(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        source.Append("    private static void WriteBinary_")
            .Append(type.SafeIdentifier)
            .Append("(global::System.Span<byte> destination, ref int position, ")
            .Append(type.TypeName)
            .AppendLine(" value)");
        source.AppendLine("    {");
        source.Append("        WriteVarint(destination, ref position, (ulong)")
            .Append(type.Members.Length.ToString(System.Globalization.CultureInfo.InvariantCulture))
            .AppendLine(");");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            string valueVariable = "value" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            source.Append("        WriteLengthPrefixedUtf8(destination, ref position, ")
                .Append(SymbolDisplay.FormatLiteral(member.JsonName, quote: true))
                .AppendLine("u8);");
            source.Append("        var ")
                .Append(valueVariable)
                .Append(" = value.")
                .Append(member.EscapedClrName)
                .AppendLine(";");
            EmitWriteBinaryValue(source, member.Value, valueVariable, "        ", i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryTypeWriters(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryTypeWriters(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryTypeWriter(source, value.ObjectType, emittedTypes);

        if (value.Element is not null)
            EmitNestedBinaryTypeWriters(source, value.Element, emittedTypes);
    }

    private static void EmitBinaryTypeReader(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        source.Append("    private static ")
            .Append(type.TypeName)
            .Append(" ReadBinaryObject_")
            .Append(type.SafeIdentifier)
            .AppendLine("(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == NullTag)");
        source.AppendLine("            return default!;");
        source.AppendLine();
        source.AppendLine("        EnsureTag(tag, ObjectTag);");
        source.Append("        return ReadBinary_")
            .Append(type.SafeIdentifier)
            .AppendLine("(payload, ref position);");
        source.AppendLine("    }");
        source.AppendLine();

        source.Append("    private static ")
            .Append(type.TypeName)
            .Append(" ReadBinary_")
            .Append(type.SafeIdentifier)
            .AppendLine("(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        ulong fieldCount = ReadVarint(payload, ref position);");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            source.Append("        ")
                .Append(member.Value.TypeName)
                .Append(" value")
                .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine(" = default!;");
        }

        if (type.Members.Length > 0)
            source.AppendLine();

        source.AppendLine("        for (ulong i = 0; i < fieldCount; i++)");
        source.AppendLine("        {");
        source.AppendLine("            global::System.ReadOnlySpan<byte> fieldName = ReadLengthPrefixedBytes(payload, ref position);");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            source.Append("            ")
                .Append(i == 0 ? "if" : "else if")
                .Append(" (fieldName.SequenceEqual(")
                .Append(SymbolDisplay.FormatLiteral(member.JsonName, quote: true))
                .AppendLine("u8))");
            source.AppendLine("            {");
            source.Append("                value")
                .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .Append(" = ");
            AppendReadBinaryValueExpression(
                source,
                member.Value,
                "payload",
                "position",
                CreateBinaryReaderMethodName(type, i));
            source.AppendLine(";");
            source.AppendLine("                continue;");
            source.AppendLine("            }");
        }

        source.AppendLine();
        source.AppendLine("            SkipBinaryValue(payload, ref position);");
        source.AppendLine("        }");
        source.AppendLine();
        source.Append("        return new ")
            .Append(type.TypeName)
            .Append('(');
        for (int i = 0; i < type.Constructor.ParameterMemberIndexes.Length; i++)
        {
            if (i > 0)
                source.Append(", ");

            source.Append("value")
                .Append(type.Constructor.ParameterMemberIndexes[i].ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        source.AppendLine(");");
        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            if (member.Value.IsArray)
                EmitBinaryArrayReader(source, member.Value, CreateBinaryReaderMethodName(type, i));
        }

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryTypeReaders(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryTypeReaders(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryTypeReader(source, value.ObjectType, emittedTypes);

        if (value.Element is not null)
            EmitNestedBinaryTypeReaders(source, value.Element, emittedTypes);
    }

    private static void EmitBinaryRecordTypeSizer(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        source.Append("    private static int GetBinaryRecordSize_")
            .Append(type.SafeIdentifier)
            .Append('(')
            .Append(type.TypeName)
            .AppendLine(" value)");
        source.AppendLine("    {");
        source.Append("        int size = ")
            .Append(GetNullBitmapSize(type).ToString(System.Globalization.CultureInfo.InvariantCulture))
            .AppendLine(";");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            string valueVariable = "value" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            source.Append("        var ")
                .Append(valueVariable)
                .Append(" = value.")
                .Append(member.EscapedClrName)
                .AppendLine(";");
            EmitGetBinaryRecordValueSize(source, member.Value, valueVariable, "        ");
        }

        source.AppendLine("        return size;");
        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryRecordTypeSizers(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryRecordTypeSizers(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryRecordTypeSizer(source, value.ObjectType, emittedTypes);
    }

    private static void EmitBinaryRecordTypeWriter(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        int nullBitmapSize = GetNullBitmapSize(type);
        source.Append("    private static void WriteBinaryRecord_")
            .Append(type.SafeIdentifier)
            .Append("(global::System.Span<byte> destination, ref int position, ")
            .Append(type.TypeName)
            .AppendLine(" value)");
        source.AppendLine("    {");
        source.AppendLine("        int nullBitmapStart = position;");
        if (nullBitmapSize > 0)
        {
            source.Append("        destination.Slice(position, ")
                .Append(nullBitmapSize.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine(").Clear();");
            source.Append("        position += ")
                .Append(nullBitmapSize.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine(";");
        }

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            string valueVariable = "value" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            source.Append("        var ")
                .Append(valueVariable)
                .Append(" = value.")
                .Append(member.EscapedClrName)
                .AppendLine(";");
            EmitWriteBinaryRecordValue(source, member.Value, valueVariable, i, "        ");
        }

        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryRecordTypeWriters(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryRecordTypeWriters(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryRecordTypeWriter(source, value.ObjectType, emittedTypes);
    }

    private static void EmitBinaryRecordTypeReader(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        source.Append("    private static ")
            .Append(type.TypeName)
            .Append(" ReadBinaryRecord_")
            .Append(type.SafeIdentifier)
            .AppendLine("(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.Append("        int nullBitmapStart = ReadBinaryRecordNullBitmap(payload, ref position, ")
            .Append(GetNullBitmapSize(type).ToString(System.Globalization.CultureInfo.InvariantCulture))
            .AppendLine(");");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            source.Append("        ")
                .Append(member.Value.TypeName)
                .Append(" value")
                .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine(" = default!;");
        }

        if (type.Members.Length > 0)
            source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            source.Append("        if (!IsBinaryRecordNull(payload, nullBitmapStart, ")
                .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine("))");
            source.AppendLine("        {");
            source.Append("            value")
                .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .Append(" = ");
            AppendReadBinaryRecordValueExpression(source, member.Value, "payload", "position");
            source.AppendLine(";");
            source.AppendLine("        }");
        }

        source.AppendLine();
        source.Append("        return new ")
            .Append(type.TypeName)
            .Append('(');
        for (int i = 0; i < type.Constructor.ParameterMemberIndexes.Length; i++)
        {
            if (i > 0)
                source.Append(", ");

            source.Append("value")
                .Append(type.Constructor.ParameterMemberIndexes[i].ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        source.AppendLine(");");
        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryRecordTypeReaders(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryRecordTypeReaders(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryRecordTypeReader(source, value.ObjectType, emittedTypes);
    }

    private static void EmitBinaryRecordTypeSkipper(
        StringBuilder source,
        BinaryTypeSpec type,
        HashSet<string> emittedTypes)
    {
        if (!emittedTypes.Add(type.SafeIdentifier))
            return;

        source.Append("    private static void SkipBinaryRecord_")
            .Append(type.SafeIdentifier)
            .AppendLine("(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.Append("        int nullBitmapStart = ReadBinaryRecordNullBitmap(payload, ref position, ")
            .Append(GetNullBitmapSize(type).ToString(System.Globalization.CultureInfo.InvariantCulture))
            .AppendLine(");");

        for (int i = 0; i < type.Members.Length; i++)
        {
            BinaryMemberSpec member = type.Members[i];
            source.Append("        if (!IsBinaryRecordNull(payload, nullBitmapStart, ")
                .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine("))");
            source.AppendLine("        {");
            EmitSkipBinaryRecordValue(source, member.Value, "payload", "position", "            ");
            source.AppendLine("        }");
        }

        source.AppendLine("    }");
        source.AppendLine();

        for (int i = 0; i < type.Members.Length; i++)
            EmitNestedBinaryRecordTypeSkippers(source, type.Members[i].Value, emittedTypes);
    }

    private static void EmitNestedBinaryRecordTypeSkippers(
        StringBuilder source,
        BinaryValueSpec value,
        HashSet<string> emittedTypes)
    {
        if (value.ObjectType is not null)
            EmitBinaryRecordTypeSkipper(source, value.ObjectType, emittedTypes);
    }

    private static int GetNullBitmapSize(BinaryTypeSpec type)
        => (type.Members.Length + 7) / 8;

    private static void EmitGetBinaryRecordValueSize(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        if (value.IsArray)
            throw new InvalidOperationException("Generated binary record format does not support array payloads.");

        if (value.CanBeNull)
        {
            source.Append(indent)
                .Append("if (")
                .Append(GetNullCheckExpression(value, expression))
                .AppendLine(")");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            EmitGetBinaryRecordNonNullValueSize(source, value, GetNonNullExpression(value, expression), indent + "    ");
            source.Append(indent).AppendLine("}");
            return;
        }

        EmitGetBinaryRecordNonNullValueSize(source, value, expression, indent);
    }

    private static void EmitGetBinaryRecordNonNullValueSize(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        if (value.ValueKindName == "Object")
        {
            BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");
            source.Append(indent)
                .Append("size += GetBinaryRecordSize_")
                .Append(objectType.SafeIdentifier)
                .Append('(')
                .Append(expression)
                .AppendLine(");");
            return;
        }

        switch (value.ValueKindName)
        {
            case "String":
                source.Append(indent)
                    .Append("size += GetLengthPrefixedStringSize(")
                    .Append(expression)
                    .AppendLine(");");
                return;
            case "Guid":
                source.Append(indent).AppendLine("size += 16;");
                return;
            case "DateOnly":
            case "Int32":
            case "UInt32":
            case "Single":
                source.Append(indent).AppendLine("size += sizeof(int);");
                return;
            case "TimeOnly":
            case "Enum":
            case "Int64":
            case "Double":
                source.Append(indent).AppendLine("size += sizeof(long);");
                return;
            case "Boolean":
            case "Byte":
            case "SByte":
                source.Append(indent).AppendLine("size += 1;");
                return;
            case "Int16":
            case "UInt16":
                source.Append(indent).AppendLine("size += sizeof(short);");
                return;
            case "Decimal":
                source.Append(indent).AppendLine("size += sizeof(int) * 4;");
                return;
            default:
                throw new InvalidOperationException($"Unsupported generated binary record value kind '{value.ValueKindName}'.");
        }
    }

    private static void EmitWriteBinaryRecordValue(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        int memberIndex,
        string indent)
    {
        if (value.IsArray)
            throw new InvalidOperationException("Generated binary record format does not support array payloads.");

        if (value.CanBeNull)
        {
            source.Append(indent)
                .Append("if (")
                .Append(GetNullCheckExpression(value, expression))
                .AppendLine(")");
            source.Append(indent).AppendLine("{");
            source.Append(indent)
                .Append("    SetBinaryRecordNull(destination, nullBitmapStart, ")
                .Append(memberIndex.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine(");");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            EmitWriteBinaryRecordNonNullValue(source, value, GetNonNullExpression(value, expression), indent + "    ");
            source.Append(indent).AppendLine("}");
            return;
        }

        EmitWriteBinaryRecordNonNullValue(source, value, expression, indent);
    }

    private static void EmitWriteBinaryRecordNonNullValue(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        if (value.ValueKindName == "Object")
        {
            BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");
            source.Append(indent)
                .Append("WriteBinaryRecord_")
                .Append(objectType.SafeIdentifier)
                .Append("(destination, ref position, ")
                .Append(expression)
                .AppendLine(");");
            return;
        }

        string methodName = value.ValueKindName switch
        {
            "String" => "WriteLengthPrefixedString",
            "Guid" => "WriteBinaryRecordGuid",
            "DateOnly" => "WriteBinaryRecordDateOnly",
            "TimeOnly" => "WriteBinaryRecordTimeOnly",
            "Enum" => "WriteBinaryRecordEnum",
            "Boolean" => "WriteBinaryRecordBoolean",
            "Byte" => "WriteBinaryRecordByte",
            "SByte" => "WriteBinaryRecordSByte",
            "Int16" => "WriteBinaryRecordInt16",
            "UInt16" => "WriteBinaryRecordUInt16",
            "Int32" => "WriteBinaryRecordInt32",
            "UInt32" => "WriteBinaryRecordUInt32",
            "Int64" => "WriteInt64",
            "Single" => "WriteBinaryRecordSingle",
            "Double" => "WriteBinaryRecordDouble",
            "Decimal" => "WriteBinaryRecordDecimal",
            _ => throw new InvalidOperationException($"Unsupported generated binary record value kind '{value.ValueKindName}'."),
        };

        source.Append(indent)
            .Append(methodName)
            .Append("(destination, ref position, ")
            .Append(expression)
            .AppendLine(");");
    }

    private static void AppendReadBinaryRecordValueExpression(
        StringBuilder source,
        BinaryValueSpec value,
        string payloadExpression,
        string positionVariable)
    {
        if (value.IsArray)
            throw new InvalidOperationException("Generated binary record format does not support array payloads.");

        if (value.ValueKindName == "Object")
        {
            BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");
            source.Append("ReadBinaryRecord_")
                .Append(objectType.SafeIdentifier)
                .Append('(')
                .Append(payloadExpression)
                .Append(", ref ")
                .Append(positionVariable)
                .Append(')');
            return;
        }

        string methodName = value.ValueKindName switch
        {
            "String" => "ReadBinaryRecordString",
            "Guid" => "ReadBinaryRecordGuid",
            "DateOnly" => "ReadBinaryRecordDateOnly",
            "TimeOnly" => "ReadBinaryRecordTimeOnly",
            "Enum" => "ReadBinaryRecordEnum<" + value.EffectiveTypeName + ">",
            "Boolean" => "ReadBinaryRecordBoolean",
            "Byte" => "ReadBinaryRecordByte",
            "SByte" => "ReadBinaryRecordSByte",
            "Int16" => "ReadBinaryRecordInt16",
            "UInt16" => "ReadBinaryRecordUInt16",
            "Int32" => "ReadBinaryRecordInt32",
            "UInt32" => "ReadBinaryRecordUInt32",
            "Int64" => "ReadInt64",
            "Single" => "ReadBinaryRecordSingle",
            "Double" => "ReadBinaryRecordDouble",
            "Decimal" => "ReadBinaryRecordDecimal",
            _ => throw new InvalidOperationException($"Unsupported generated binary record value kind '{value.ValueKindName}'."),
        };

        source.Append(methodName)
            .Append('(')
            .Append(payloadExpression)
            .Append(", ref ")
            .Append(positionVariable)
            .Append(')');
        if (value.ValueKindName == "String")
            source.Append('!');
    }

    private static void EmitSkipBinaryRecordValue(
        StringBuilder source,
        BinaryValueSpec value,
        string payloadExpression,
        string positionVariable,
        string indent)
    {
        if (value.IsArray)
            throw new InvalidOperationException("Generated binary record format does not support array payloads.");

        if (value.ValueKindName == "Object")
        {
            BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");
            source.Append(indent)
                .Append("SkipBinaryRecord_")
                .Append(objectType.SafeIdentifier)
                .Append('(')
                .Append(payloadExpression)
                .Append(", ref ")
                .Append(positionVariable)
                .AppendLine(");");
            return;
        }

        switch (value.ValueKindName)
        {
            case "String":
                source.Append(indent)
                    .Append("_ = ReadLengthPrefixedBytes(")
                    .Append(payloadExpression)
                    .Append(", ref ")
                    .Append(positionVariable)
                    .AppendLine(");");
                return;
            case "Guid":
                source.Append(indent)
                    .Append("EnsureAvailable(")
                    .Append(payloadExpression)
                    .Append(", ")
                    .Append(positionVariable)
                    .AppendLine(", 16);");
                source.Append(indent)
                    .Append(positionVariable)
                    .AppendLine(" += 16;");
                return;
            case "DateOnly":
            case "Int32":
            case "UInt32":
            case "Single":
                source.Append(indent)
                    .Append("EnsureAvailable(")
                    .Append(payloadExpression)
                    .Append(", ")
                    .Append(positionVariable)
                    .AppendLine(", sizeof(int));");
                source.Append(indent)
                    .Append(positionVariable)
                    .AppendLine(" += sizeof(int);");
                return;
            case "TimeOnly":
            case "Enum":
            case "Int64":
            case "Double":
                source.Append(indent)
                    .Append("EnsureAvailable(")
                    .Append(payloadExpression)
                    .Append(", ")
                    .Append(positionVariable)
                    .AppendLine(", sizeof(long));");
                source.Append(indent)
                    .Append(positionVariable)
                    .AppendLine(" += sizeof(long);");
                return;
            case "Boolean":
            case "Byte":
            case "SByte":
                source.Append(indent)
                    .Append("EnsureAvailable(")
                    .Append(payloadExpression)
                    .Append(", ")
                    .Append(positionVariable)
                    .AppendLine(", 1);");
                source.Append(indent)
                    .Append(positionVariable)
                    .AppendLine("++;");
                return;
            case "Int16":
            case "UInt16":
                source.Append(indent)
                    .Append("EnsureAvailable(")
                    .Append(payloadExpression)
                    .Append(", ")
                    .Append(positionVariable)
                    .AppendLine(", sizeof(short));");
                source.Append(indent)
                    .Append(positionVariable)
                    .AppendLine(" += sizeof(short);");
                return;
            case "Decimal":
                source.Append(indent)
                    .Append("EnsureAvailable(")
                    .Append(payloadExpression)
                    .Append(", ")
                    .Append(positionVariable)
                    .AppendLine(", sizeof(int) * 4);");
                source.Append(indent)
                    .Append(positionVariable)
                    .AppendLine(" += sizeof(int) * 4;");
                return;
            default:
                throw new InvalidOperationException($"Unsupported generated binary record value kind '{value.ValueKindName}'.");
        }
    }

    private static string GetNullCheckExpression(BinaryValueSpec value, string expression)
        => value.IsNullableValueType
            ? "!" + expression + ".HasValue"
            : expression + " is null";

    private static string GetNonNullExpression(BinaryValueSpec value, string expression)
        => value.IsNullableValueType ? expression + ".Value" : expression;

    private static void EmitBinaryPayloadFieldReaders(
        StringBuilder source,
        BinaryTypeSpec root,
        ImmutableArray<CollectionFieldSpec> fields)
    {
        for (int i = 0; i < fields.Length; i++)
        {
            CollectionFieldSpec field = fields[i];
            if (!TryCreateBinaryFieldReaderSpec(root, field, out BinaryFieldReaderSpec reader))
                continue;

            EmitBinaryPayloadFieldReader(source, root, field, reader);
        }
    }

    private static void EmitBinaryPayloadFieldReader(
        StringBuilder source,
        BinaryTypeSpec root,
        CollectionFieldSpec field,
        BinaryFieldReaderSpec reader)
    {
        source.Append("    internal static bool ")
            .Append(CreateBinaryPayloadValueReaderName(field))
            .AppendLine("(global::System.ReadOnlySpan<byte> payload, out global::CSharpDB.Primitives.DbValue value)");
            source.AppendLine("    {");
            source.AppendLine("        value = default;");
            EmitBinaryPayloadFieldPathReader(
                source,
                root,
                reader,
                "Value",
                "        return TryReadBinaryPayloadFieldValue(currentPayload, ref position, out value);");
            source.AppendLine("    }");
            source.AppendLine();

        if (reader.DataKindName == "Integer")
        {
            source.Append("    internal static bool ")
                .Append(CreateBinaryPayloadIntegerReaderName(field))
                .AppendLine("(global::System.ReadOnlySpan<byte> payload, out long value)");
            source.AppendLine("    {");
                source.AppendLine("        value = 0;");
                EmitBinaryPayloadFieldPathReader(
                    source,
                    root,
                    reader,
                    "Integer",
                    "        return TryReadBinaryPayloadFieldInt64(currentPayload, ref position, out value);");
                source.AppendLine("    }");
                source.AppendLine();
        }
        else if (reader.DataKindName == "Text")
        {
            source.Append("    internal static bool ")
                .Append(CreateBinaryPayloadTextReaderName(field))
                .AppendLine("(global::System.ReadOnlySpan<byte> payload, out string? value)");
            source.AppendLine("    {");
                source.AppendLine("        value = null;");
                EmitBinaryPayloadFieldPathReader(
                    source,
                    root,
                    reader,
                    "Text",
                    "        return TryReadBinaryPayloadFieldText(currentPayload, ref position, out value);");
                source.AppendLine("    }");
                source.AppendLine();

            source.Append("    internal static bool ")
                .Append(CreateBinaryPayloadTextUtf8ReaderName(field))
                .AppendLine("(global::System.ReadOnlySpan<byte> payload, out global::System.ReadOnlySpan<byte> value)");
            source.AppendLine("    {");
                source.AppendLine("        value = default;");
                EmitBinaryPayloadFieldPathReader(
                    source,
                    root,
                    reader,
                    "TextUtf8",
                    "        return TryReadBinaryPayloadFieldTextUtf8(currentPayload, ref position, out value);");
                source.AppendLine("    }");
                source.AppendLine();
        }
    }

    private static void EmitBinaryPayloadFieldPathReader(
        StringBuilder source,
        BinaryTypeSpec root,
        BinaryFieldReaderSpec reader,
        string recordReadKindName,
        string finalReadStatement)
    {
        source.AppendLine("        if (!global::CSharpDB.Storage.Serialization.CollectionPayloadCodec.TryGetBinaryDocumentPayload(payload, out global::System.ReadOnlySpan<byte> currentPayload))");
        source.AppendLine("            return false;");
        source.AppendLine();
        source.AppendLine("        int position = 0;");

        if (CanUseBinaryRecordFormat(root))
        {
            source.AppendLine("        if (IsBinaryRecordPayload(currentPayload))");
            source.AppendLine("        {");
            source.AppendLine("            position = 3;");
            EmitBinaryRecordPayloadFieldPathReader(source, root, reader, recordReadKindName);
            source.AppendLine("        }");
            source.AppendLine();
            source.AppendLine("        position = 0;");
        }

        BinaryTypeSpec currentType = root;
        for (int depth = 0; depth < reader.MemberIndexes.Length; depth++)
        {
            int memberIndex = reader.MemberIndexes[depth];
            BinaryMemberSpec member = currentType.Members[memberIndex];

            source.Append("        if (!TryReadExpectedObjectFieldCount(currentPayload, ref position, ")
                .Append(currentType.Members.Length.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine("))");
            source.AppendLine("            return false;");

            for (int i = 0; i < memberIndex; i++)
            {
                source.AppendLine("        SkipBinaryField(currentPayload, ref position);");
            }

            source.Append("        if (!TryReadExpectedFieldName(currentPayload, ref position, ")
                .Append(SymbolDisplay.FormatLiteral(member.JsonName, quote: true))
                .AppendLine("u8))");
            source.AppendLine("            return false;");

            if (depth == reader.MemberIndexes.Length - 1)
            {
                source.AppendLine(finalReadStatement);
            }
            else
            {
                source.Append("        if (!TryReadBinaryObjectPayload(currentPayload, ref position, out int nestedStart")
                    .Append(depth.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    .Append(", out int nestedLength")
                    .Append(depth.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    .AppendLine("))");
                source.AppendLine("            return false;");
                source.Append("        currentPayload = currentPayload.Slice(nestedStart")
                    .Append(depth.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    .Append(", nestedLength")
                    .Append(depth.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    .AppendLine(");");
                source.AppendLine("        position = 0;");
                source.AppendLine();
                currentType = member.Value.ObjectType ?? throw new InvalidOperationException("Binary field reader path entered a non-object member.");
            }
        }
    }

    private static void EmitBinaryRecordPayloadFieldPathReader(
        StringBuilder source,
        BinaryTypeSpec root,
        BinaryFieldReaderSpec reader,
        string recordReadKindName)
    {
        BinaryTypeSpec currentType = root;
        for (int depth = 0; depth < reader.MemberIndexes.Length; depth++)
        {
            int memberIndex = reader.MemberIndexes[depth];
            BinaryMemberSpec member = currentType.Members[memberIndex];
            string nullBitmapStart = "nullBitmapStart" + depth.ToString(System.Globalization.CultureInfo.InvariantCulture);

            source.Append("            int ")
                .Append(nullBitmapStart)
                .Append(" = ReadBinaryRecordNullBitmap(currentPayload, ref position, ")
                .Append(GetNullBitmapSize(currentType).ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine(");");
            source.Append("            if (IsBinaryRecordNull(currentPayload, ")
                .Append(nullBitmapStart)
                .Append(", ")
                .Append(memberIndex.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .AppendLine("))");
            source.AppendLine("                return false;");

            for (int i = 0; i < memberIndex; i++)
            {
                source.Append("            if (!IsBinaryRecordNull(currentPayload, ")
                    .Append(nullBitmapStart)
                    .Append(", ")
                    .Append(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    .AppendLine("))");
                source.AppendLine("            {");
                EmitSkipBinaryRecordValue(source, currentType.Members[i].Value, "currentPayload", "position", "                ");
                source.AppendLine("            }");
            }

            if (depth == reader.MemberIndexes.Length - 1)
            {
                EmitBinaryRecordPayloadFinalRead(source, reader, recordReadKindName, "            ");
            }
            else
            {
                currentType = member.Value.ObjectType ?? throw new InvalidOperationException("Binary field reader path entered a non-object member.");
            }
        }
    }

    private static void EmitBinaryRecordPayloadFinalRead(
        StringBuilder source,
        BinaryFieldReaderSpec reader,
        string recordReadKindName,
        string indent)
    {
        switch (recordReadKindName)
        {
            case "Value":
                if (reader.DataKindName == "Integer")
                {
                    source.Append(indent).Append("value = global::CSharpDB.Primitives.DbValue.FromInteger(");
                    AppendReadBinaryRecordIntegerAsInt64Expression(source, reader.Value, "currentPayload", "position");
                    source.AppendLine(");");
                }
                else if (reader.DataKindName == "Text")
                {
                    source.Append(indent).Append("value = global::CSharpDB.Primitives.DbValue.FromText(");
                    AppendReadBinaryRecordTextExpression(source, reader.Value, "currentPayload", "position");
                    source.AppendLine(");");
                }
                else
                {
                    throw new InvalidOperationException($"Unsupported generated binary record field data kind '{reader.DataKindName}'.");
                }

                source.Append(indent).AppendLine("return true;");
                return;

            case "Integer":
                source.Append(indent).Append("value = ");
                AppendReadBinaryRecordIntegerAsInt64Expression(source, reader.Value, "currentPayload", "position");
                source.AppendLine(";");
                source.Append(indent).AppendLine("return true;");
                return;

            case "Text":
                source.Append(indent).Append("value = ");
                AppendReadBinaryRecordTextExpression(source, reader.Value, "currentPayload", "position");
                source.AppendLine(";");
                source.Append(indent).AppendLine("return true;");
                return;

            case "TextUtf8":
                if (reader.Value.ValueKindName == "String")
                {
                    source.Append(indent).AppendLine("value = ReadBinaryRecordStringUtf8(currentPayload, ref position);");
                    source.Append(indent).AppendLine("return true;");
                }
                else
                {
                    source.Append(indent).AppendLine("return false;");
                }

                return;

            default:
                throw new InvalidOperationException($"Unsupported generated binary record read kind '{recordReadKindName}'.");
        }
    }

    private static void AppendReadBinaryRecordIntegerAsInt64Expression(
        StringBuilder source,
        BinaryValueSpec value,
        string payloadExpression,
        string positionVariable)
    {
        switch (value.ValueKindName)
        {
            case "Byte":
            case "SByte":
            case "Int16":
            case "UInt16":
            case "Int32":
            case "UInt32":
                source.Append("(long)");
                AppendReadBinaryRecordValueExpression(source, value, payloadExpression, positionVariable);
                return;
            case "Int64":
            case "Enum":
                source.Append("ReadInt64(")
                    .Append(payloadExpression)
                    .Append(", ref ")
                    .Append(positionVariable)
                    .Append(')');
                return;
            default:
                throw new InvalidOperationException($"Unsupported generated binary record integer field kind '{value.ValueKindName}'.");
        }
    }

    private static void AppendReadBinaryRecordTextExpression(
        StringBuilder source,
        BinaryValueSpec value,
        string payloadExpression,
        string positionVariable)
    {
        switch (value.ValueKindName)
        {
            case "String":
                source.Append("ReadBinaryRecordString(")
                    .Append(payloadExpression)
                    .Append(", ref ")
                    .Append(positionVariable)
                    .Append(')');
                return;
            case "Guid":
                source.Append("ReadBinaryRecordGuid(")
                    .Append(payloadExpression)
                    .Append(", ref ")
                    .Append(positionVariable)
                    .Append(").ToString(\"D\")");
                return;
            case "DateOnly":
                source.Append("ReadBinaryRecordDateOnly(")
                    .Append(payloadExpression)
                    .Append(", ref ")
                    .Append(positionVariable)
                    .Append(").ToString(\"O\", global::System.Globalization.CultureInfo.InvariantCulture)");
                return;
            case "TimeOnly":
                source.Append("ReadBinaryRecordTimeOnly(")
                    .Append(payloadExpression)
                    .Append(", ref ")
                    .Append(positionVariable)
                    .Append(").ToString(\"O\", global::System.Globalization.CultureInfo.InvariantCulture)");
                return;
            default:
                throw new InvalidOperationException($"Unsupported generated binary record text field kind '{value.ValueKindName}'.");
        }
    }

    private static string CreateBinaryReaderMethodName(BinaryTypeSpec type, int memberIndex)
        => "ReadBinaryArray_" + type.SafeIdentifier + "_" + memberIndex.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static void EmitBinaryArrayReader(
        StringBuilder source,
        BinaryValueSpec value,
        string methodName)
    {
        BinaryValueSpec element = value.Element ?? throw new InvalidOperationException("Array binary value is missing its element spec.");

        source.Append("    private static ")
            .Append(value.TypeName)
            .Append(' ')
            .Append(methodName)
            .AppendLine("(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == NullTag)");
        source.AppendLine("            return default!;");
        source.AppendLine();
        source.AppendLine("        EnsureTag(tag, ArrayTag);");
        source.AppendLine("        ulong count = ReadVarint(payload, ref position);");
        source.Append("        var values = new global::System.Collections.Generic.List<")
            .Append(element.TypeName)
            .AppendLine(">(checked((int)count));");
        source.AppendLine("        for (ulong i = 0; i < count; i++)");
        source.AppendLine("        {");
        source.Append("            values.Add(");
        AppendReadBinaryValueExpression(source, element, "payload", "position", methodName + "_Element");
        source.AppendLine(");");
        source.AppendLine("        }");
        source.AppendLine();
        if (value.IsArrayType)
            source.AppendLine("        return values.ToArray();");
        else
            source.AppendLine("        return values;");
        source.AppendLine("    }");
        source.AppendLine();
    }

    private static void AppendReadBinaryValueExpression(
        StringBuilder source,
        BinaryValueSpec value,
        string payloadExpression,
        string positionVariable,
        string arrayReaderMethodName)
    {
        if (value.IsArray)
        {
            source.Append(arrayReaderMethodName)
                .Append('(')
                .Append(payloadExpression)
                .Append(", ref ")
                .Append(positionVariable)
                .Append(')');
            return;
        }

        if (value.ValueKindName == "Object")
        {
            BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");
            source.Append("ReadBinaryObject_")
                .Append(objectType.SafeIdentifier)
                .Append('(')
                .Append(payloadExpression)
                .Append(", ref ")
                .Append(positionVariable)
                .Append(')');
            return;
        }

        string methodName = value.ValueKindName switch
        {
            "String" => "ReadBinaryString",
            "Guid" => value.IsNullableValueType ? "ReadNullableBinaryGuid" : "ReadBinaryGuid",
            "DateOnly" => value.IsNullableValueType ? "ReadNullableBinaryDateOnly" : "ReadBinaryDateOnly",
            "TimeOnly" => value.IsNullableValueType ? "ReadNullableBinaryTimeOnly" : "ReadBinaryTimeOnly",
            "Enum" => value.IsNullableValueType ? "ReadNullableBinaryEnum<" + value.EffectiveTypeName + ">" : "ReadBinaryEnum<" + value.EffectiveTypeName + ">",
            "Boolean" => value.IsNullableValueType ? "ReadNullableBinaryBoolean" : "ReadBinaryBoolean",
            "Byte" => value.IsNullableValueType ? "ReadNullableBinaryByte" : "ReadBinaryByte",
            "SByte" => value.IsNullableValueType ? "ReadNullableBinarySByte" : "ReadBinarySByte",
            "Int16" => value.IsNullableValueType ? "ReadNullableBinaryInt16" : "ReadBinaryInt16",
            "UInt16" => value.IsNullableValueType ? "ReadNullableBinaryUInt16" : "ReadBinaryUInt16",
            "Int32" => value.IsNullableValueType ? "ReadNullableBinaryInt32" : "ReadBinaryInt32",
            "UInt32" => value.IsNullableValueType ? "ReadNullableBinaryUInt32" : "ReadBinaryUInt32",
            "Int64" => value.IsNullableValueType ? "ReadNullableBinaryInt64" : "ReadBinaryInt64",
            "Single" => value.IsNullableValueType ? "ReadNullableBinarySingle" : "ReadBinarySingle",
            "Double" => value.IsNullableValueType ? "ReadNullableBinaryDouble" : "ReadBinaryDouble",
            "Decimal" => value.IsNullableValueType ? "ReadNullableBinaryDecimal" : "ReadBinaryDecimal",
            _ => throw new InvalidOperationException($"Unsupported generated binary value kind '{value.ValueKindName}'."),
        };

        source.Append(methodName)
            .Append('(')
            .Append(payloadExpression)
            .Append(", ref ")
            .Append(positionVariable)
            .Append(')');
        if (value.ValueKindName == "String")
            source.Append('!');
    }

    private static void EmitGetBinaryValueSize(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent,
        string suffix)
    {
        if (value.IsArray)
        {
            EmitGetBinaryArrayValueSize(source, value, expression, indent, suffix);
            return;
        }

        if (value.ValueKindName == "Object")
        {
            EmitGetBinaryObjectValueSize(source, value, expression, indent);
            return;
        }

        if (value.IsNullableValueType)
        {
            source.Append(indent)
                .Append("if (!")
                .Append(expression)
                .AppendLine(".HasValue)");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    size += 1;");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            EmitGetBinaryScalarValueSize(source, value, expression + ".Value", indent + "    ");
            source.Append(indent).AppendLine("}");
            return;
        }

        EmitGetBinaryScalarValueSize(source, value, expression, indent);
    }

    private static void EmitGetBinaryArrayValueSize(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent,
        string suffix)
    {
        BinaryValueSpec element = value.Element ?? throw new InvalidOperationException("Array binary value is missing its element spec.");
        string countVariable = "count" + suffix;
        string bufferVariable = "buffer" + suffix;
        string itemVariable = "item" + suffix;

        source.Append(indent)
            .Append("if (")
            .Append(expression)
            .AppendLine(" is null)");
        source.Append(indent).AppendLine("{");
        source.Append(indent).AppendLine("    size += 1;");
        source.Append(indent).AppendLine("}");
        source.Append(indent).AppendLine("else");
        source.Append(indent).AppendLine("{");
        source.Append(indent).AppendLine("    size += 1;");
        source.Append(indent)
            .Append("    if (!global::System.Linq.Enumerable.TryGetNonEnumeratedCount<")
            .Append(element.TypeName)
            .Append(">(")
            .Append(expression)
            .Append(", out int ")
            .Append(countVariable)
            .AppendLine("))");
        source.Append(indent).AppendLine("    {");
        source.Append(indent)
            .Append("        var ")
            .Append(bufferVariable)
            .Append(" = new global::System.Collections.Generic.List<")
            .Append(element.TypeName)
            .AppendLine(">();");
        source.Append(indent)
            .Append("        foreach (var ")
            .Append(itemVariable)
            .Append(" in ")
            .Append(expression)
            .AppendLine(")");
        source.Append(indent)
            .Append("            ")
            .Append(bufferVariable)
            .Append(".Add(")
            .Append(itemVariable)
            .AppendLine(");");
        source.Append(indent)
            .Append("        size += GetVarintSize((ulong)")
            .Append(bufferVariable)
            .AppendLine(".Count);");
        source.Append(indent)
            .Append("        foreach (var ")
            .Append(itemVariable)
            .Append(" in ")
            .Append(bufferVariable)
            .AppendLine(")");
        source.Append(indent).AppendLine("        {");
        EmitGetBinaryValueSize(source, element, itemVariable, indent + "            ", suffix + "_item");
        source.Append(indent).AppendLine("        }");
        source.Append(indent).AppendLine("    }");
        source.Append(indent).AppendLine("    else");
        source.Append(indent).AppendLine("    {");
        source.Append(indent)
            .Append("        size += GetVarintSize((ulong)")
            .Append(countVariable)
            .AppendLine(");");
        source.Append(indent)
            .Append("        foreach (var ")
            .Append(itemVariable)
            .Append(" in ")
            .Append(expression)
            .AppendLine(")");
        source.Append(indent).AppendLine("        {");
        EmitGetBinaryValueSize(source, element, itemVariable, indent + "            ", suffix + "_item");
        source.Append(indent).AppendLine("        }");
        source.Append(indent).AppendLine("    }");
        source.Append(indent).AppendLine("}");
    }

    private static void EmitGetBinaryObjectValueSize(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");

        if (value.IsNullableValueType)
        {
            source.Append(indent)
                .Append("if (!")
                .Append(expression)
                .AppendLine(".HasValue)");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    size += 1;");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            source.Append(indent)
                .Append("    size += 1 + GetBinarySize_")
                .Append(objectType.SafeIdentifier)
                .Append('(')
                .Append(expression)
                .AppendLine(".Value);");
            source.Append(indent).AppendLine("}");
            return;
        }

        if (value.CanBeNull)
        {
            source.Append(indent)
                .Append("if (")
                .Append(expression)
                .AppendLine(" is null)");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    size += 1;");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            source.Append(indent)
                .Append("    size += 1 + GetBinarySize_")
                .Append(objectType.SafeIdentifier)
                .Append('(')
                .Append(expression)
                .AppendLine(");");
            source.Append(indent).AppendLine("}");
            return;
        }

        source.Append(indent)
            .Append("size += 1 + GetBinarySize_")
            .Append(objectType.SafeIdentifier)
            .Append('(')
            .Append(expression)
            .AppendLine(");");
    }

    private static void EmitGetBinaryScalarValueSize(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        switch (value.ValueKindName)
        {
            case "String":
                source.Append(indent)
                    .Append("size += GetBinaryStringSize(")
                    .Append(expression)
                    .AppendLine(");");
                return;
            case "Guid":
                source.Append(indent)
                    .Append("size += GetBinaryStringSize(")
                    .Append(expression)
                    .AppendLine(".ToString(\"D\"));");
                return;
            case "DateOnly":
            case "TimeOnly":
                source.Append(indent)
                    .Append("size += GetBinaryStringSize(")
                    .Append(expression)
                    .AppendLine(".ToString(\"O\", global::System.Globalization.CultureInfo.InvariantCulture));");
                return;
            case "Boolean":
                source.Append(indent).AppendLine("size += 1;");
                return;
            case "Enum":
            case "Byte":
            case "SByte":
            case "Int16":
            case "UInt16":
            case "Int32":
            case "UInt32":
            case "Int64":
            case "Single":
            case "Double":
                source.Append(indent).AppendLine("size += 1 + sizeof(long);");
                return;
            case "Decimal":
                source.Append(indent).AppendLine("size += 1 + (sizeof(int) * 4);");
                return;
            default:
                throw new InvalidOperationException($"Unsupported generated binary value kind '{value.ValueKindName}'.");
        }
    }

    private static void EmitWriteBinaryValue(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent,
        string suffix)
    {
        if (value.IsArray)
        {
            EmitWriteBinaryArrayValue(source, value, expression, indent, suffix);
            return;
        }

        if (value.ValueKindName == "Object")
        {
            EmitWriteBinaryObjectValue(source, value, expression, indent);
            return;
        }

        if (value.IsNullableValueType)
        {
            source.Append(indent)
                .Append("if (!")
                .Append(expression)
                .AppendLine(".HasValue)");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    WriteByte(destination, ref position, NullTag);");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            EmitWriteBinaryScalarValue(source, value, expression + ".Value", indent + "    ");
            source.Append(indent).AppendLine("}");
            return;
        }

        EmitWriteBinaryScalarValue(source, value, expression, indent);
    }

    private static void EmitWriteBinaryArrayValue(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent,
        string suffix)
    {
        BinaryValueSpec element = value.Element ?? throw new InvalidOperationException("Array binary value is missing its element spec.");
        string countVariable = "count" + suffix;
        string bufferVariable = "buffer" + suffix;
        string itemVariable = "item" + suffix;

        source.Append(indent)
            .Append("if (")
            .Append(expression)
            .AppendLine(" is null)");
        source.Append(indent).AppendLine("{");
        source.Append(indent).AppendLine("    WriteByte(destination, ref position, NullTag);");
        source.Append(indent).AppendLine("}");
        source.Append(indent).AppendLine("else");
        source.Append(indent).AppendLine("{");
        source.Append(indent).AppendLine("    WriteByte(destination, ref position, ArrayTag);");
        source.Append(indent)
            .Append("    if (!global::System.Linq.Enumerable.TryGetNonEnumeratedCount<")
            .Append(element.TypeName)
            .Append(">(")
            .Append(expression)
            .Append(", out int ")
            .Append(countVariable)
            .AppendLine("))");
        source.Append(indent).AppendLine("    {");
        source.Append(indent)
            .Append("        var ")
            .Append(bufferVariable)
            .Append(" = new global::System.Collections.Generic.List<")
            .Append(element.TypeName)
            .AppendLine(">();");
        source.Append(indent)
            .Append("        foreach (var ")
            .Append(itemVariable)
            .Append(" in ")
            .Append(expression)
            .AppendLine(")");
        source.Append(indent)
            .Append("            ")
            .Append(bufferVariable)
            .Append(".Add(")
            .Append(itemVariable)
            .AppendLine(");");
        source.Append(indent)
            .Append("        WriteVarint(destination, ref position, (ulong)")
            .Append(bufferVariable)
            .AppendLine(".Count);");
        source.Append(indent)
            .Append("        foreach (var ")
            .Append(itemVariable)
            .Append(" in ")
            .Append(bufferVariable)
            .AppendLine(")");
        source.Append(indent).AppendLine("        {");
        EmitWriteBinaryValue(source, element, itemVariable, indent + "            ", suffix + "_item");
        source.Append(indent).AppendLine("        }");
        source.Append(indent).AppendLine("    }");
        source.Append(indent).AppendLine("    else");
        source.Append(indent).AppendLine("    {");
        source.Append(indent)
            .Append("        WriteVarint(destination, ref position, (ulong)")
            .Append(countVariable)
            .AppendLine(");");
        source.Append(indent)
            .Append("        foreach (var ")
            .Append(itemVariable)
            .Append(" in ")
            .Append(expression)
            .AppendLine(")");
        source.Append(indent).AppendLine("        {");
        EmitWriteBinaryValue(source, element, itemVariable, indent + "            ", suffix + "_item");
        source.Append(indent).AppendLine("        }");
        source.Append(indent).AppendLine("    }");
        source.Append(indent).AppendLine("}");
    }

    private static void EmitWriteBinaryObjectValue(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        BinaryTypeSpec objectType = value.ObjectType ?? throw new InvalidOperationException("Object binary value is missing its type spec.");

        if (value.IsNullableValueType)
        {
            source.Append(indent)
                .Append("if (!")
                .Append(expression)
                .AppendLine(".HasValue)");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    WriteByte(destination, ref position, NullTag);");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    WriteByte(destination, ref position, ObjectTag);");
            source.Append(indent)
                .Append("    WriteBinary_")
                .Append(objectType.SafeIdentifier)
                .Append("(destination, ref position, ")
                .Append(expression)
                .AppendLine(".Value);");
            source.Append(indent).AppendLine("}");
            return;
        }

        if (value.CanBeNull)
        {
            source.Append(indent)
                .Append("if (")
                .Append(expression)
                .AppendLine(" is null)");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    WriteByte(destination, ref position, NullTag);");
            source.Append(indent).AppendLine("}");
            source.Append(indent).AppendLine("else");
            source.Append(indent).AppendLine("{");
            source.Append(indent).AppendLine("    WriteByte(destination, ref position, ObjectTag);");
            source.Append(indent)
                .Append("    WriteBinary_")
                .Append(objectType.SafeIdentifier)
                .Append("(destination, ref position, ")
                .Append(expression)
                .AppendLine(");");
            source.Append(indent).AppendLine("}");
            return;
        }

        source.Append(indent).AppendLine("WriteByte(destination, ref position, ObjectTag);");
        source.Append(indent)
            .Append("WriteBinary_")
            .Append(objectType.SafeIdentifier)
            .Append("(destination, ref position, ")
            .Append(expression)
            .AppendLine(");");
    }

    private static void EmitWriteBinaryScalarValue(
        StringBuilder source,
        BinaryValueSpec value,
        string expression,
        string indent)
    {
        switch (value.ValueKindName)
        {
            case "String":
                source.Append(indent)
                    .Append("WriteBinaryString(destination, ref position, ")
                    .Append(expression)
                    .AppendLine(");");
                return;
            case "Guid":
                source.Append(indent)
                    .Append("WriteBinaryString(destination, ref position, ")
                    .Append(expression)
                    .AppendLine(".ToString(\"D\"));");
                return;
            case "DateOnly":
            case "TimeOnly":
                source.Append(indent)
                    .Append("WriteBinaryString(destination, ref position, ")
                    .Append(expression)
                    .AppendLine(".ToString(\"O\", global::System.Globalization.CultureInfo.InvariantCulture));");
                return;
            case "Enum":
                source.Append(indent)
                    .Append("WriteBinaryInteger(destination, ref position, global::System.Convert.ToInt64(")
                    .Append(expression)
                    .AppendLine(", global::System.Globalization.CultureInfo.InvariantCulture));");
                return;
            case "Boolean":
                source.Append(indent)
                    .Append("WriteByte(destination, ref position, ")
                    .Append(expression)
                    .AppendLine(" ? TrueTag : FalseTag);");
                return;
            case "Byte":
            case "SByte":
            case "Int16":
            case "UInt16":
            case "Int32":
            case "UInt32":
            case "Int64":
                source.Append(indent)
                    .Append("WriteBinaryInteger(destination, ref position, (long)")
                    .Append(expression)
                    .AppendLine(");");
                return;
            case "Single":
                source.Append(indent)
                    .Append("WriteBinaryDouble(destination, ref position, (double)")
                    .Append(expression)
                    .AppendLine(");");
                return;
            case "Double":
                source.Append(indent)
                    .Append("WriteBinaryDouble(destination, ref position, ")
                    .Append(expression)
                    .AppendLine(");");
                return;
            case "Decimal":
                source.Append(indent)
                    .Append("WriteBinaryDecimal(destination, ref position, ")
                    .Append(expression)
                    .AppendLine(");");
                return;
            default:
                throw new InvalidOperationException($"Unsupported generated binary value kind '{value.ValueKindName}'.");
        }
    }

    private static void EmitBinaryPrimitiveHelpers(StringBuilder source)
    {
        source.AppendLine("    private static int GetBinaryStringSize(string? value)");
        source.AppendLine("        => value is null ? 1 : 1 + GetLengthPrefixedStringSize(value);");
        source.AppendLine();
        source.AppendLine("    private static int GetLengthPrefixedStringSize(string value)");
        source.AppendLine("    {");
        source.AppendLine("        int byteCount = global::System.Text.Encoding.UTF8.GetByteCount(value);");
        source.AppendLine("        return GetVarintSize((ulong)byteCount) + byteCount;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static int GetLengthPrefixedUtf8Size(global::System.ReadOnlySpan<byte> value)");
        source.AppendLine("        => GetVarintSize((ulong)value.Length) + value.Length;");
        source.AppendLine();
        source.AppendLine("    private static int GetVarintSize(ulong value)");
        source.AppendLine("        => global::CSharpDB.Storage.Serialization.Varint.SizeOf(value);");
        source.AppendLine();
        source.AppendLine("    private static bool IsBinaryRecordPayload(global::System.ReadOnlySpan<byte> payload)");
        source.AppendLine("        => payload.Length >= 3 && payload[0] == RecordFormatMarker && payload[1] == RecordFormatMagic && payload[2] == RecordFormatVersion;");
        source.AppendLine();
        source.AppendLine("    private static int ReadBinaryRecordNullBitmap(global::System.ReadOnlySpan<byte> payload, ref int position, int byteCount)");
        source.AppendLine("    {");
        source.AppendLine("        int start = position;");
        source.AppendLine("        EnsureAvailable(payload, position, byteCount);");
        source.AppendLine("        position += byteCount;");
        source.AppendLine("        return start;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool IsBinaryRecordNull(global::System.ReadOnlySpan<byte> payload, int nullBitmapStart, int fieldIndex)");
        source.AppendLine("        => (payload[nullBitmapStart + (fieldIndex >> 3)] & (1 << (fieldIndex & 7))) != 0;");
        source.AppendLine();
        source.AppendLine("    private static void SetBinaryRecordNull(global::System.Span<byte> destination, int nullBitmapStart, int fieldIndex)");
        source.AppendLine("        => destination[nullBitmapStart + (fieldIndex >> 3)] = (byte)(destination[nullBitmapStart + (fieldIndex >> 3)] | (1 << (fieldIndex & 7)));");
        source.AppendLine();
        source.AppendLine("    private static string ReadBinaryRecordString(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.Text.Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static global::System.ReadOnlySpan<byte> ReadBinaryRecordStringUtf8(global::System.ReadOnlySpan<byte> payload, scoped ref int position)");
        source.AppendLine("        => ReadLengthPrefixedBytes(payload, ref position);");
        source.AppendLine();
        source.AppendLine("    private static global::System.Guid ReadBinaryRecordGuid(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, 16);");
        source.AppendLine("        global::System.Guid value = new global::System.Guid(payload.Slice(position, 16));");
        source.AppendLine("        position += 16;");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static global::System.DateOnly ReadBinaryRecordDateOnly(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.DateOnly.FromDayNumber(ReadBinaryRecordInt32(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static global::System.TimeOnly ReadBinaryRecordTimeOnly(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => new global::System.TimeOnly(ReadInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static TEnum ReadBinaryRecordEnum<TEnum>(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        where TEnum : struct");
        source.AppendLine("        => (TEnum)global::System.Enum.ToObject(typeof(TEnum), ReadInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static bool ReadBinaryRecordBoolean(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => ReadByte(payload, ref position) != 0;");
        source.AppendLine();
        source.AppendLine("    private static byte ReadBinaryRecordByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => ReadByte(payload, ref position);");
        source.AppendLine();
        source.AppendLine("    private static sbyte ReadBinaryRecordSByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => unchecked((sbyte)ReadByte(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static short ReadBinaryRecordInt16(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(short));");
        source.AppendLine("        short value = global::System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(payload[position..]);");
        source.AppendLine("        position += sizeof(short);");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static ushort ReadBinaryRecordUInt16(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(ushort));");
        source.AppendLine("        ushort value = global::System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(payload[position..]);");
        source.AppendLine("        position += sizeof(ushort);");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static int ReadBinaryRecordInt32(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(int));");
        source.AppendLine("        int value = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[position..]);");
        source.AppendLine("        position += sizeof(int);");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static uint ReadBinaryRecordUInt32(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(uint));");
        source.AppendLine("        uint value = global::System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload[position..]);");
        source.AppendLine("        position += sizeof(uint);");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static float ReadBinaryRecordSingle(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.BitConverter.Int32BitsToSingle(ReadBinaryRecordInt32(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static double ReadBinaryRecordDouble(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.BitConverter.Int64BitsToDouble(ReadInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static decimal ReadBinaryRecordDecimal(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(int) * 4);");
        source.AppendLine("        int lo = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[position..]);");
        source.AppendLine("        int mid = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 4)..]);");
        source.AppendLine("        int hi = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 8)..]);");
        source.AppendLine("        int flags = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 12)..]);");
        source.AppendLine("        position += sizeof(int) * 4;");
        source.AppendLine("        return new decimal(new int[] { lo, mid, hi, flags });");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static string? ReadBinaryString(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == NullTag)");
        source.AppendLine("            return null;");
        source.AppendLine();
        source.AppendLine("        EnsureTag(tag, StringTag);");
        source.AppendLine("        return global::System.Text.Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position));");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static string ReadRequiredBinaryString(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => ReadBinaryString(payload, ref position)");
        source.AppendLine("           ?? throw new global::CSharpDB.Primitives.CSharpDbException(global::CSharpDB.Primitives.ErrorCode.CorruptDatabase, \"Generated binary collection payload contained null for a required string value.\");");
        source.AppendLine();
        source.AppendLine("    private static global::System.Guid ReadBinaryGuid(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.Guid.Parse(ReadRequiredBinaryString(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static global::System.Guid? ReadNullableBinaryGuid(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        string? value = ReadBinaryString(payload, ref position);");
        source.AppendLine("        return value is null ? null : global::System.Guid.Parse(value);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static global::System.DateOnly ReadBinaryDateOnly(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.DateOnly.ParseExact(ReadRequiredBinaryString(payload, ref position), \"O\", global::System.Globalization.CultureInfo.InvariantCulture);");
        source.AppendLine();
        source.AppendLine("    private static global::System.DateOnly? ReadNullableBinaryDateOnly(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        string? value = ReadBinaryString(payload, ref position);");
        source.AppendLine("        return value is null ? null : global::System.DateOnly.ParseExact(value, \"O\", global::System.Globalization.CultureInfo.InvariantCulture);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static global::System.TimeOnly ReadBinaryTimeOnly(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => global::System.TimeOnly.ParseExact(ReadRequiredBinaryString(payload, ref position), \"O\", global::System.Globalization.CultureInfo.InvariantCulture);");
        source.AppendLine();
        source.AppendLine("    private static global::System.TimeOnly? ReadNullableBinaryTimeOnly(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        string? value = ReadBinaryString(payload, ref position);");
        source.AppendLine("        return value is null ? null : global::System.TimeOnly.ParseExact(value, \"O\", global::System.Globalization.CultureInfo.InvariantCulture);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static TEnum ReadBinaryEnum<TEnum>(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        where TEnum : struct");
        source.AppendLine("        => (TEnum)global::System.Enum.ToObject(typeof(TEnum), ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static TEnum? ReadNullableBinaryEnum<TEnum>(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        where TEnum : struct");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? (TEnum)global::System.Enum.ToObject(typeof(TEnum), value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool ReadBinaryBoolean(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == TrueTag)");
        source.AppendLine("            return true;");
        source.AppendLine("        if (tag == FalseTag)");
        source.AppendLine("            return false;");
        source.AppendLine();
        source.AppendLine("        ThrowUnexpectedTag(tag, TrueTag);");
        source.AppendLine("        return false;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool? ReadNullableBinaryBoolean(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == NullTag)");
        source.AppendLine("            return null;");
        source.AppendLine("        if (tag == TrueTag)");
        source.AppendLine("            return true;");
        source.AppendLine("        if (tag == FalseTag)");
        source.AppendLine("            return false;");
        source.AppendLine();
        source.AppendLine("        ThrowUnexpectedTag(tag, TrueTag);");
        source.AppendLine("        return null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static byte ReadBinaryByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((byte)ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static byte? ReadNullableBinaryByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((byte)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static sbyte ReadBinarySByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((sbyte)ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static sbyte? ReadNullableBinarySByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((sbyte)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static short ReadBinaryInt16(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((short)ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static short? ReadNullableBinaryInt16(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((short)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static ushort ReadBinaryUInt16(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((ushort)ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static ushort? ReadNullableBinaryUInt16(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((ushort)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static int ReadBinaryInt32(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((int)ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static int? ReadNullableBinaryInt32(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((int)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static uint ReadBinaryUInt32(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((uint)ReadBinaryInt64(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static uint? ReadNullableBinaryUInt32(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        long? value = ReadNullableBinaryInt64(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((uint)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static long ReadBinaryInt64(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => ReadBinaryIntegerPayload(payload, ref position, ReadByte(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static long? ReadNullableBinaryInt64(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        return tag == NullTag ? null : ReadBinaryIntegerPayload(payload, ref position, tag);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static long ReadBinaryIntegerPayload(global::System.ReadOnlySpan<byte> payload, ref int position, byte tag)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureTag(tag, IntegerTag);");
        source.AppendLine("        return ReadInt64(payload, ref position);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static float ReadBinarySingle(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => checked((float)ReadBinaryDouble(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static float? ReadNullableBinarySingle(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        double? value = ReadNullableBinaryDouble(payload, ref position);");
        source.AppendLine("        return value.HasValue ? checked((float)value.Value) : null;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static double ReadBinaryDouble(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => ReadBinaryDoublePayload(payload, ref position, ReadByte(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static double? ReadNullableBinaryDouble(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        return tag == NullTag ? null : ReadBinaryDoublePayload(payload, ref position, tag);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static double ReadBinaryDoublePayload(global::System.ReadOnlySpan<byte> payload, ref int position, byte tag)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureTag(tag, DoubleTag);");
        source.AppendLine("        return global::System.BitConverter.Int64BitsToDouble(ReadInt64(payload, ref position));");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static decimal ReadBinaryDecimal(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("        => ReadBinaryDecimalPayload(payload, ref position, ReadByte(payload, ref position));");
        source.AppendLine();
        source.AppendLine("    private static decimal? ReadNullableBinaryDecimal(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        return tag == NullTag ? null : ReadBinaryDecimalPayload(payload, ref position, tag);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static decimal ReadBinaryDecimalPayload(global::System.ReadOnlySpan<byte> payload, ref int position, byte tag)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureTag(tag, DecimalTag);");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(int) * 4);");
        source.AppendLine("        int lo = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[position..]);");
        source.AppendLine("        int mid = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 4)..]);");
        source.AppendLine("        int hi = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 8)..]);");
        source.AppendLine("        int flags = global::System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 12)..]);");
        source.AppendLine("        position += sizeof(int) * 4;");
        source.AppendLine("        return new decimal(new int[] { lo, mid, hi, flags });");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void SkipBinaryValue(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        switch (tag)");
        source.AppendLine("        {");
        source.AppendLine("            case NullTag:");
        source.AppendLine("            case FalseTag:");
        source.AppendLine("            case TrueTag:");
        source.AppendLine("                return;");
        source.AppendLine("            case StringTag:");
        source.AppendLine("                _ = ReadLengthPrefixedBytes(payload, ref position);");
        source.AppendLine("                return;");
        source.AppendLine("            case IntegerTag:");
        source.AppendLine("            case DoubleTag:");
        source.AppendLine("                EnsureAvailable(payload, position, sizeof(long));");
        source.AppendLine("                position += sizeof(long);");
        source.AppendLine("                return;");
        source.AppendLine("            case DecimalTag:");
        source.AppendLine("                EnsureAvailable(payload, position, sizeof(int) * 4);");
        source.AppendLine("                position += sizeof(int) * 4;");
        source.AppendLine("                return;");
        source.AppendLine("            case ObjectTag:");
        source.AppendLine("                SkipBinaryObject(payload, ref position);");
        source.AppendLine("                return;");
        source.AppendLine("            case ArrayTag:");
        source.AppendLine("                ulong elementCount = ReadVarint(payload, ref position);");
        source.AppendLine("                for (ulong i = 0; i < elementCount; i++)");
        source.AppendLine("                    SkipBinaryValue(payload, ref position);");
        source.AppendLine("                return;");
        source.AppendLine("            default:");
        source.AppendLine("                ThrowUnexpectedTag(tag, NullTag);");
        source.AppendLine("                return;");
        source.AppendLine("        }");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void SkipBinaryObject(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        ulong fieldCount = ReadVarint(payload, ref position);");
        source.AppendLine("        for (ulong i = 0; i < fieldCount; i++)");
        source.AppendLine("        {");
        source.AppendLine("            _ = ReadLengthPrefixedBytes(payload, ref position);");
        source.AppendLine("            SkipBinaryValue(payload, ref position);");
        source.AppendLine("        }");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static byte ReadByte(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, 1);");
        source.AppendLine("        return payload[position++];");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static ulong ReadVarint(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, 1);");
        source.AppendLine("        ulong value = global::CSharpDB.Storage.Serialization.Varint.Read(payload[position..], out int bytesRead);");
        source.AppendLine("        EnsureAvailable(payload, position, bytesRead);");
        source.AppendLine("        position += bytesRead;");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static global::System.ReadOnlySpan<byte> ReadLengthPrefixedBytes(global::System.ReadOnlySpan<byte> payload, scoped ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        int length = checked((int)ReadVarint(payload, ref position));");
        source.AppendLine("        EnsureAvailable(payload, position, length);");
        source.AppendLine("        global::System.ReadOnlySpan<byte> value = payload.Slice(position, length);");
        source.AppendLine("        position += length;");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadExpectedFieldName(global::System.ReadOnlySpan<byte> payload, ref int position, global::System.ReadOnlySpan<byte> expected)");
        source.AppendLine("    {");
        source.AppendLine("        int start = position;");
        source.AppendLine("        global::System.ReadOnlySpan<byte> fieldName = ReadLengthPrefixedBytes(payload, ref position);");
        source.AppendLine("        if (!fieldName.SequenceEqual(expected))");
        source.AppendLine("        {");
        source.AppendLine("            position = start;");
        source.AppendLine("            return false;");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        return true;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadExpectedObjectFieldCount(global::System.ReadOnlySpan<byte> payload, ref int position, int expectedFieldCount)");
        source.AppendLine("    {");
        source.AppendLine("        int current = position;");
        source.AppendLine("        ulong fieldCount = ReadVarint(payload, ref current);");
        source.AppendLine("        if (fieldCount != (ulong)expectedFieldCount)");
        source.AppendLine("            return false;");
        source.AppendLine();
        source.AppendLine("        position = current;");
        source.AppendLine("        return true;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TrySkipExpectedBinaryField(global::System.ReadOnlySpan<byte> payload, ref int position, global::System.ReadOnlySpan<byte> expected)");
        source.AppendLine("    {");
        source.AppendLine("        if (!TryReadExpectedFieldName(payload, ref position, expected))");
        source.AppendLine("            return false;");
        source.AppendLine();
        source.AppendLine("        SkipBinaryValue(payload, ref position);");
        source.AppendLine("        return true;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void SkipBinaryField(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        _ = ReadLengthPrefixedBytes(payload, ref position);");
        source.AppendLine("        SkipBinaryValue(payload, ref position);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadBinaryObjectPayload(global::System.ReadOnlySpan<byte> payload, ref int position, out int objectStart, out int objectLength)");
        source.AppendLine("    {");
        source.AppendLine("        objectStart = 0;");
        source.AppendLine("        objectLength = 0;");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag != ObjectTag)");
        source.AppendLine("            return false;");
        source.AppendLine();
        source.AppendLine("        objectStart = position;");
        source.AppendLine("        SkipBinaryObject(payload, ref position);");
        source.AppendLine("        objectLength = position - objectStart;");
        source.AppendLine("        return true;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadBinaryPayloadFieldValue(global::System.ReadOnlySpan<byte> payload, ref int position, out global::CSharpDB.Primitives.DbValue value)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        switch (tag)");
        source.AppendLine("        {");
        source.AppendLine("            case StringTag:");
        source.AppendLine("                value = global::CSharpDB.Primitives.DbValue.FromText(global::System.Text.Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position)));");
        source.AppendLine("                return true;");
        source.AppendLine("            case IntegerTag:");
        source.AppendLine("                value = global::CSharpDB.Primitives.DbValue.FromInteger(ReadInt64(payload, ref position));");
        source.AppendLine("                return true;");
        source.AppendLine("            case DoubleTag:");
        source.AppendLine("                value = global::CSharpDB.Primitives.DbValue.FromReal(global::System.BitConverter.Int64BitsToDouble(ReadInt64(payload, ref position)));");
        source.AppendLine("                return true;");
        source.AppendLine("            default:");
        source.AppendLine("                value = default;");
        source.AppendLine("                return false;");
        source.AppendLine("        }");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadBinaryPayloadFieldInt64(global::System.ReadOnlySpan<byte> payload, ref int position, out long value)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == IntegerTag)");
        source.AppendLine("        {");
        source.AppendLine("            value = ReadInt64(payload, ref position);");
        source.AppendLine("            return true;");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        value = 0;");
        source.AppendLine("        return false;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadBinaryPayloadFieldText(global::System.ReadOnlySpan<byte> payload, ref int position, out string? value)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == StringTag)");
        source.AppendLine("        {");
        source.AppendLine("            value = global::System.Text.Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position));");
        source.AppendLine("            return true;");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        value = null;");
        source.AppendLine("        return false;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static bool TryReadBinaryPayloadFieldTextUtf8(global::System.ReadOnlySpan<byte> payload, scoped ref int position, out global::System.ReadOnlySpan<byte> value)");
        source.AppendLine("    {");
        source.AppendLine("        byte tag = ReadByte(payload, ref position);");
        source.AppendLine("        if (tag == StringTag)");
        source.AppendLine("        {");
        source.AppendLine("            value = ReadLengthPrefixedBytes(payload, ref position);");
        source.AppendLine("            return true;");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        value = default;");
        source.AppendLine("        return false;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static long ReadInt64(global::System.ReadOnlySpan<byte> payload, ref int position)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(payload, position, sizeof(long));");
        source.AppendLine("        long value = global::System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(payload[position..]);");
        source.AppendLine("        position += sizeof(long);");
        source.AppendLine("        return value;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void EnsureTag(byte actual, byte expected)");
        source.AppendLine("    {");
        source.AppendLine("        if (actual != expected)");
        source.AppendLine("            ThrowUnexpectedTag(actual, expected);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void EnsureAvailable(global::System.ReadOnlySpan<byte> payload, int position, int count)");
        source.AppendLine("    {");
        source.AppendLine("        if (count < 0 || position < 0 || payload.Length - position < count)");
        source.AppendLine("            throw new global::CSharpDB.Primitives.CSharpDbException(global::CSharpDB.Primitives.ErrorCode.CorruptDatabase, \"Invalid generated binary collection payload length.\");");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void ThrowUnexpectedTag(byte actual, byte expected)");
        source.AppendLine("        => throw new global::CSharpDB.Primitives.CSharpDbException(global::CSharpDB.Primitives.ErrorCode.CorruptDatabase, $\"Invalid generated binary collection payload tag {actual}; expected {expected}.\");");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryString(global::System.Span<byte> destination, ref int position, string? value)");
        source.AppendLine("    {");
        source.AppendLine("        if (value is null)");
        source.AppendLine("        {");
        source.AppendLine("            WriteByte(destination, ref position, NullTag);");
        source.AppendLine("            return;");
        source.AppendLine("        }");
        source.AppendLine();
        source.AppendLine("        WriteByte(destination, ref position, StringTag);");
        source.AppendLine("        WriteLengthPrefixedString(destination, ref position, value);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordGuid(global::System.Span<byte> destination, ref int position, global::System.Guid value)");
        source.AppendLine("    {");
        source.AppendLine("        EnsureAvailable(destination, position, 16);");
        source.AppendLine("        if (!value.TryWriteBytes(destination.Slice(position, 16)))");
        source.AppendLine("            throw new global::System.InvalidOperationException(\"Failed to write generated binary record Guid value.\");");
        source.AppendLine("        position += 16;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordDateOnly(global::System.Span<byte> destination, ref int position, global::System.DateOnly value)");
        source.AppendLine("        => WriteBinaryRecordInt32(destination, ref position, value.DayNumber);");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordTimeOnly(global::System.Span<byte> destination, ref int position, global::System.TimeOnly value)");
        source.AppendLine("        => WriteInt64(destination, ref position, value.Ticks);");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordEnum<TEnum>(global::System.Span<byte> destination, ref int position, TEnum value)");
        source.AppendLine("        where TEnum : struct");
        source.AppendLine("        => WriteInt64(destination, ref position, global::System.Convert.ToInt64(value, global::System.Globalization.CultureInfo.InvariantCulture));");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordBoolean(global::System.Span<byte> destination, ref int position, bool value)");
        source.AppendLine("        => WriteByte(destination, ref position, value ? (byte)1 : (byte)0);");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordByte(global::System.Span<byte> destination, ref int position, byte value)");
        source.AppendLine("        => WriteByte(destination, ref position, value);");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordSByte(global::System.Span<byte> destination, ref int position, sbyte value)");
        source.AppendLine("        => WriteByte(destination, ref position, unchecked((byte)value));");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordInt16(global::System.Span<byte> destination, ref int position, short value)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt16LittleEndian(destination.Slice(position, sizeof(short)), value);");
        source.AppendLine("        position += sizeof(short);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordUInt16(global::System.Span<byte> destination, ref int position, ushort value)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(position, sizeof(ushort)), value);");
        source.AppendLine("        position += sizeof(ushort);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordInt32(global::System.Span<byte> destination, ref int position, int value)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(position, sizeof(int)), value);");
        source.AppendLine("        position += sizeof(int);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordUInt32(global::System.Span<byte> destination, ref int position, uint value)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(position, sizeof(uint)), value);");
        source.AppendLine("        position += sizeof(uint);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordSingle(global::System.Span<byte> destination, ref int position, float value)");
        source.AppendLine("        => WriteBinaryRecordInt32(destination, ref position, global::System.BitConverter.SingleToInt32Bits(value));");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordDouble(global::System.Span<byte> destination, ref int position, double value)");
        source.AppendLine("        => WriteInt64(destination, ref position, global::System.BitConverter.DoubleToInt64Bits(value));");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryRecordDecimal(global::System.Span<byte> destination, ref int position, decimal value)");
        source.AppendLine("    {");
        source.AppendLine("        int[] bits = decimal.GetBits(value);");
        source.AppendLine("        global::System.Span<byte> span = destination.Slice(position, sizeof(int) * 4);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span, bits[0]);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[4..], bits[1]);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[8..], bits[2]);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[12..], bits[3]);");
        source.AppendLine("        position += sizeof(int) * 4;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryInteger(global::System.Span<byte> destination, ref int position, long value)");
        source.AppendLine("    {");
        source.AppendLine("        WriteByte(destination, ref position, IntegerTag);");
        source.AppendLine("        WriteInt64(destination, ref position, value);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryDouble(global::System.Span<byte> destination, ref int position, double value)");
        source.AppendLine("    {");
        source.AppendLine("        WriteByte(destination, ref position, DoubleTag);");
        source.AppendLine("        WriteInt64(destination, ref position, global::System.BitConverter.DoubleToInt64Bits(value));");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBinaryDecimal(global::System.Span<byte> destination, ref int position, decimal value)");
        source.AppendLine("    {");
        source.AppendLine("        WriteByte(destination, ref position, DecimalTag);");
        source.AppendLine("        int[] bits = decimal.GetBits(value);");
        source.AppendLine("        global::System.Span<byte> span = destination.Slice(position, sizeof(int) * 4);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span, bits[0]);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[4..], bits[1]);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[8..], bits[2]);");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span[12..], bits[3]);");
        source.AppendLine("        position += sizeof(int) * 4;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteByte(global::System.Span<byte> destination, ref int position, byte value)");
        source.AppendLine("    {");
        source.AppendLine("        destination[position] = value;");
        source.AppendLine("        position++;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteVarint(global::System.Span<byte> destination, ref int position, ulong value)");
        source.AppendLine("    {");
        source.AppendLine("        position += global::CSharpDB.Storage.Serialization.Varint.Write(destination[position..], value);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteBytes(global::System.Span<byte> destination, ref int position, global::System.ReadOnlySpan<byte> value)");
        source.AppendLine("    {");
        source.AppendLine("        value.CopyTo(destination[position..]);");
        source.AppendLine("        position += value.Length;");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteLengthPrefixedString(global::System.Span<byte> destination, ref int position, string value)");
        source.AppendLine("    {");
        source.AppendLine("        int byteCount = global::System.Text.Encoding.UTF8.GetByteCount(value);");
        source.AppendLine("        WriteVarint(destination, ref position, (ulong)byteCount);");
        source.AppendLine("        position += global::System.Text.Encoding.UTF8.GetBytes(value.AsSpan(), destination.Slice(position, byteCount));");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteLengthPrefixedUtf8(global::System.Span<byte> destination, ref int position, global::System.ReadOnlySpan<byte> value)");
        source.AppendLine("    {");
        source.AppendLine("        WriteVarint(destination, ref position, (ulong)value.Length);");
        source.AppendLine("        WriteBytes(destination, ref position, value);");
        source.AppendLine("    }");
        source.AppendLine();
        source.AppendLine("    private static void WriteInt64(global::System.Span<byte> destination, ref int position, long value)");
        source.AppendLine("    {");
        source.AppendLine("        global::System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(position, sizeof(long)), value);");
        source.AppendLine("        position += sizeof(long);");
        source.AppendLine("    }");
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
            ImmutableArray<CollectionFieldSpec> fields,
            BinaryTypeSpec? binaryModel)
        {
            NamespaceName = namespaceName;
            TypeName = typeName;
            FullyQualifiedTypeName = fullyQualifiedTypeName;
            JsonContextTypeName = jsonContextTypeName;
            PartialTypeKeyword = partialTypeKeyword;
            SafeIdentifier = safeIdentifier;
            Fields = fields;
            BinaryModel = binaryModel;
        }

        public string? NamespaceName { get; }

        public string TypeName { get; }

        public string FullyQualifiedTypeName { get; }

        public string JsonContextTypeName { get; }

        public string PartialTypeKeyword { get; }

        public string SafeIdentifier { get; }

        public ImmutableArray<CollectionFieldSpec> Fields { get; }

        public BinaryTypeSpec? BinaryModel { get; }
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

    private sealed class BinaryTypeSpec
    {
        public BinaryTypeSpec(
            string typeName,
            string safeIdentifier,
            ImmutableArray<BinaryMemberSpec> members,
            BinaryConstructorSpec constructor)
        {
            TypeName = typeName;
            SafeIdentifier = safeIdentifier;
            Members = members;
            Constructor = constructor;
        }

        public string TypeName { get; }

        public string SafeIdentifier { get; }

        public ImmutableArray<BinaryMemberSpec> Members { get; }

        public BinaryConstructorSpec Constructor { get; }
    }

    private readonly struct BinaryConstructorSpec
    {
        public BinaryConstructorSpec(ImmutableArray<int> parameterMemberIndexes)
        {
            ParameterMemberIndexes = parameterMemberIndexes;
        }

        public ImmutableArray<int> ParameterMemberIndexes { get; }
    }

    private readonly struct BinaryFieldReaderSpec
    {
        public BinaryFieldReaderSpec(
            ImmutableArray<int> memberIndexes,
            BinaryValueSpec value,
            string dataKindName)
        {
            MemberIndexes = memberIndexes;
            Value = value;
            DataKindName = dataKindName;
        }

        public ImmutableArray<int> MemberIndexes { get; }

        public BinaryValueSpec Value { get; }

        public string DataKindName { get; }
    }

    private readonly struct BinaryMemberCandidate
    {
        public BinaryMemberCandidate(
            ISymbol member,
            ITypeSymbol type,
            BinaryValueSpec value)
        {
            Member = member;
            Type = type;
            Value = value;
        }

        public ISymbol Member { get; }

        public ITypeSymbol Type { get; }

        public BinaryValueSpec Value { get; }
    }

    private readonly struct BinaryMemberSpec
    {
        public BinaryMemberSpec(
            string clrName,
            string escapedClrName,
            string jsonName,
            BinaryValueSpec value)
        {
            ClrName = clrName;
            EscapedClrName = escapedClrName;
            JsonName = jsonName;
            Value = value;
        }

        public string ClrName { get; }

        public string EscapedClrName { get; }

        public string JsonName { get; }

        public BinaryValueSpec Value { get; }
    }

    private sealed class BinaryValueSpec
    {
        private BinaryValueSpec(
            string typeName,
            string effectiveTypeName,
            string valueKindName,
            bool canBeNull,
            bool isNullableValueType,
            bool isArray,
            bool isArrayType,
            BinaryValueSpec? element,
            BinaryTypeSpec? objectType)
        {
            TypeName = typeName;
            EffectiveTypeName = effectiveTypeName;
            ValueKindName = valueKindName;
            CanBeNull = canBeNull;
            IsNullableValueType = isNullableValueType;
            IsArray = isArray;
            IsArrayType = isArrayType;
            Element = element;
            ObjectType = objectType;
        }

        public static BinaryValueSpec ForScalar(
            string typeName,
            string effectiveTypeName,
            string valueKindName,
            bool canBeNull,
            bool isNullableValueType)
            => new(
                typeName,
                effectiveTypeName,
                valueKindName,
                canBeNull,
                isNullableValueType,
                isArray: false,
                isArrayType: false,
                element: null,
                objectType: null);

        public static BinaryValueSpec ForObject(
            string typeName,
            string effectiveTypeName,
            bool canBeNull,
            bool isNullableValueType,
            BinaryTypeSpec objectType)
            => new(
                typeName,
                effectiveTypeName,
                "Object",
                canBeNull,
                isNullableValueType,
                isArray: false,
                isArrayType: false,
                element: null,
                objectType);

        public static BinaryValueSpec ForArray(
            string typeName,
            bool isArrayType,
            bool canBeNull,
            BinaryValueSpec element)
            => new(
                typeName,
                typeName,
                "Array",
                canBeNull,
                isNullableValueType: false,
                isArray: true,
                isArrayType,
                element,
                objectType: null);

        public string TypeName { get; }

        public string EffectiveTypeName { get; }

        public string ValueKindName { get; }

        public bool CanBeNull { get; }

        public bool IsNullableValueType { get; }

        public bool IsArray { get; }

        public bool IsArrayType { get; }

        public BinaryValueSpec? Element { get; }

        public BinaryTypeSpec? ObjectType { get; }
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
