using System.Globalization;
using System.Text;

namespace CSharpDB.Primitives;

public sealed record AutomationStubGenerationOptions(
    string Namespace = "CSharpDbAutomation",
    string ClassName = "CSharpDbAutomationRegistration",
    string MethodName = "Register");

public static class AutomationStubGenerator
{
    public static string GenerateCSharp(
        DbAutomationMetadata metadata,
        AutomationStubGenerationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(metadata);

        options ??= new AutomationStubGenerationOptions();
        ValidateOptions(options);

        CallbackGroup[] scalarFunctions = GetScalarFunctionGroups(metadata);
        CallbackGroup[] commands = GetCommandGroups(metadata);

        var source = new StringBuilder();
        source.AppendLine("using System;");
        source.AppendLine("using System.Threading.Tasks;");
        source.AppendLine("using CSharpDB.Primitives;");
        source.AppendLine();
        source.Append("namespace ");
        source.Append(options.Namespace);
        source.AppendLine(";");
        source.AppendLine();
        source.Append("public static class ");
        source.AppendLine(options.ClassName);
        source.AppendLine("{");
        source.Append("    public static void ");
        source.Append(options.MethodName);
        source.AppendLine("(DbFunctionRegistryBuilder functions, DbCommandRegistryBuilder commands)");
        source.AppendLine("    {");
        source.AppendLine("        ArgumentNullException.ThrowIfNull(functions);");
        source.AppendLine("        ArgumentNullException.ThrowIfNull(commands);");

        bool wroteRegistration = false;
        foreach (CallbackGroup function in scalarFunctions)
        {
            source.AppendLine();
            AppendScalarFunction(source, function);
            wroteRegistration = true;
        }

        foreach (CallbackGroup command in commands)
        {
            source.AppendLine();
            AppendCommand(source, command);
            wroteRegistration = true;
        }

        if (!wroteRegistration)
        {
            source.AppendLine();
            source.AppendLine("        // No trusted C# callbacks were found in the automation metadata.");
        }

        source.AppendLine("    }");
        source.AppendLine("}");

        return source.ToString();
    }

    private static void AppendScalarFunction(StringBuilder source, CallbackGroup function)
    {
        source.AppendLine("        functions.AddScalar(");
        source.Append("            ");
        source.Append(ToStringLiteral(function.Name));
        source.AppendLine(",");
        source.Append("            arity: ");
        source.Append(function.Arity!.Value.ToString(CultureInfo.InvariantCulture));
        source.AppendLine(",");
        source.AppendLine("            options: new DbScalarFunctionOptions(DbType.Text),");
        source.AppendLine("            invoke: static (context, args) =>");
        source.AppendLine("            {");
        AppendReferenceComments(source, function);
        source.Append("                throw new NotImplementedException(");
        source.Append(ToStringLiteral($"Implement trusted scalar function '{function.Name}'."));
        source.AppendLine(");");
        source.AppendLine("            });");
    }

    private static void AppendCommand(StringBuilder source, CallbackGroup command)
    {
        source.AppendLine("        commands.AddAsyncCommand(");
        source.Append("            ");
        source.Append(ToStringLiteral(command.Name));
        source.AppendLine(",");
        source.AppendLine("            static async (context, ct) =>");
        source.AppendLine("            {");
        AppendReferenceComments(source, command);
        source.AppendLine("                await Task.CompletedTask;");
        source.Append("                throw new NotImplementedException(");
        source.Append(ToStringLiteral($"Implement trusted command '{command.Name}'."));
        source.AppendLine(");");
        source.AppendLine("            });");
    }

    private static void AppendReferenceComments(StringBuilder source, CallbackGroup callback)
    {
        source.AppendLine("                // References:");
        foreach (CallbackLocation location in callback.Locations)
        {
            source.Append("                // - ");
            source.Append(SanitizeComment(location.Surface));
            source.Append(": ");
            source.AppendLine(SanitizeComment(location.Location));
        }
    }

    private static CallbackGroup[] GetScalarFunctionGroups(DbAutomationMetadata metadata)
        => (metadata.ScalarFunctions ?? [])
            .Where(static reference => !string.IsNullOrWhiteSpace(reference.Name))
            .GroupBy(
                static reference => $"{reference.Name.Trim()}|{reference.Arity}",
                StringComparer.OrdinalIgnoreCase)
            .Select(static group =>
            {
                DbAutomationScalarFunctionReference first = group.First();
                return new CallbackGroup(
                    first.Name.Trim(),
                    first.Arity,
                    GetLocations(group.Select(static reference => new CallbackLocation(reference.Surface, reference.Location))));
            })
            .OrderBy(static group => group.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static group => group.Arity)
            .ToArray();

    private static CallbackGroup[] GetCommandGroups(DbAutomationMetadata metadata)
        => (metadata.Commands ?? [])
            .Where(static reference => !string.IsNullOrWhiteSpace(reference.Name))
            .GroupBy(
                static reference => reference.Name.Trim(),
                StringComparer.OrdinalIgnoreCase)
            .Select(static group =>
            {
                DbAutomationCommandReference first = group.First();
                return new CallbackGroup(
                    first.Name.Trim(),
                    Arity: null,
                    GetLocations(group.Select(static reference => new CallbackLocation(reference.Surface, reference.Location))));
            })
            .OrderBy(static group => group.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private static CallbackLocation[] GetLocations(IEnumerable<CallbackLocation> locations)
        => locations
            .Select(static location => new CallbackLocation(
                NormalizeCommentValue(location.Surface, "unknown"),
                NormalizeCommentValue(location.Location, "$")))
            .GroupBy(
                static location => $"{location.Surface}|{location.Location}",
                StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .OrderBy(static location => location.Surface, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static location => location.Location, StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private static string ToStringLiteral(string value)
    {
        var literal = new StringBuilder(value.Length + 2);
        literal.Append('"');
        foreach (char ch in value)
        {
            literal.Append(ch switch
            {
                '\\' => "\\\\",
                '"' => "\\\"",
                '\0' => "\\0",
                '\a' => "\\a",
                '\b' => "\\b",
                '\f' => "\\f",
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                '\v' => "\\v",
                _ when char.IsControl(ch) => "\\u" + ((int)ch).ToString("X4", CultureInfo.InvariantCulture),
                _ => ch.ToString(),
            });
        }

        literal.Append('"');
        return literal.ToString();
    }

    private static string SanitizeComment(string value)
    {
        var sanitized = new StringBuilder(value.Length);
        foreach (char ch in value)
            sanitized.Append(char.IsControl(ch) ? ' ' : ch);

        return sanitized.ToString().Trim();
    }

    private static string NormalizeCommentValue(string? value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();

    private static void ValidateOptions(AutomationStubGenerationOptions options)
    {
        ValidateNamespace(options.Namespace);
        ValidateIdentifier(options.ClassName, nameof(options.ClassName));
        ValidateIdentifier(options.MethodName, nameof(options.MethodName));
    }

    private static void ValidateNamespace(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        foreach (string part in value.Split('.'))
            ValidateIdentifier(part, nameof(AutomationStubGenerationOptions.Namespace));
    }

    private static void ValidateIdentifier(string value, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);

        if (s_csharpKeywords.Contains(value))
            throw new ArgumentException($"'{value}' is a reserved C# keyword.", parameterName);

        if (!IsIdentifierStart(value[0]))
            throw new ArgumentException($"'{value}' is not a valid C# identifier.", parameterName);

        for (int i = 1; i < value.Length; i++)
        {
            if (!IsIdentifierPart(value[i]))
                throw new ArgumentException($"'{value}' is not a valid C# identifier.", parameterName);
        }
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';

    private static bool IsIdentifierPart(char value)
        => char.IsLetterOrDigit(value) || value == '_';

    private static readonly HashSet<string> s_csharpKeywords = new(StringComparer.Ordinal)
    {
        "abstract",
        "as",
        "base",
        "bool",
        "break",
        "byte",
        "case",
        "catch",
        "char",
        "checked",
        "class",
        "const",
        "continue",
        "decimal",
        "default",
        "delegate",
        "do",
        "double",
        "else",
        "enum",
        "event",
        "explicit",
        "extern",
        "false",
        "finally",
        "fixed",
        "float",
        "for",
        "foreach",
        "goto",
        "if",
        "implicit",
        "in",
        "int",
        "interface",
        "internal",
        "is",
        "lock",
        "long",
        "namespace",
        "new",
        "null",
        "object",
        "operator",
        "out",
        "override",
        "params",
        "private",
        "protected",
        "public",
        "readonly",
        "ref",
        "return",
        "sbyte",
        "sealed",
        "short",
        "sizeof",
        "stackalloc",
        "static",
        "string",
        "struct",
        "switch",
        "this",
        "throw",
        "true",
        "try",
        "typeof",
        "uint",
        "ulong",
        "unchecked",
        "unsafe",
        "ushort",
        "using",
        "virtual",
        "void",
        "volatile",
        "while",
    };

    private sealed record CallbackGroup(
        string Name,
        int? Arity,
        IReadOnlyList<CallbackLocation> Locations);

    private sealed record CallbackLocation(string Surface, string Location);
}
