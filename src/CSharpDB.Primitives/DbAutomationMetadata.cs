namespace CSharpDB.Primitives;

public sealed record DbAutomationMetadata(
    int MetadataVersion = DbAutomationMetadata.CurrentMetadataVersion,
    IReadOnlyList<DbAutomationCommandReference>? Commands = null,
    IReadOnlyList<DbAutomationScalarFunctionReference>? ScalarFunctions = null)
{
    public const int CurrentMetadataVersion = 1;

    public bool IsEmpty => (Commands is null || Commands.Count == 0)
        && (ScalarFunctions is null || ScalarFunctions.Count == 0);
}

public sealed record DbAutomationCommandReference(
    string Name,
    string Surface,
    string Location);

public sealed record DbAutomationScalarFunctionReference(
    string Name,
    int Arity,
    string Surface,
    string Location);

public sealed record DbAutomationScalarFunctionCall(string Name, int Arity);

public sealed class DbAutomationMetadataBuilder
{
    private readonly List<DbAutomationCommandReference> _commands = [];
    private readonly List<DbAutomationScalarFunctionReference> _scalarFunctions = [];

    public DbAutomationMetadataBuilder AddCommand(string? name, string surface, string location)
    {
        if (string.IsNullOrWhiteSpace(name))
            return this;

        ArgumentException.ThrowIfNullOrWhiteSpace(surface);
        ArgumentException.ThrowIfNullOrWhiteSpace(location);
        _commands.Add(new DbAutomationCommandReference(name.Trim(), surface.Trim(), location.Trim()));
        return this;
    }

    public DbAutomationMetadataBuilder AddScalarFunction(string? name, int arity, string surface, string location)
    {
        if (string.IsNullOrWhiteSpace(name))
            return this;

        ArgumentOutOfRangeException.ThrowIfNegative(arity);
        ArgumentException.ThrowIfNullOrWhiteSpace(surface);
        ArgumentException.ThrowIfNullOrWhiteSpace(location);
        _scalarFunctions.Add(new DbAutomationScalarFunctionReference(name.Trim(), arity, surface.Trim(), location.Trim()));
        return this;
    }

    public DbAutomationMetadata Build()
        => new(
            DbAutomationMetadata.CurrentMetadataVersion,
            SortCommands(_commands),
            SortScalarFunctions(_scalarFunctions));

    private static IReadOnlyList<DbAutomationCommandReference> SortCommands(IEnumerable<DbAutomationCommandReference> commands)
        => commands
            .GroupBy(
                static command => $"{command.Name}|{command.Surface}|{command.Location}",
                StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .OrderBy(static command => command.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static command => command.Surface, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static command => command.Location, StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private static IReadOnlyList<DbAutomationScalarFunctionReference> SortScalarFunctions(IEnumerable<DbAutomationScalarFunctionReference> functions)
        => functions
            .GroupBy(
                static function => $"{function.Name}|{function.Arity}|{function.Surface}|{function.Location}",
                StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .OrderBy(static function => function.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static function => function.Arity)
            .ThenBy(static function => function.Surface, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static function => function.Location, StringComparer.OrdinalIgnoreCase)
            .ToArray();
}

public static class DbAutomationExpressionInspector
{
    public static IReadOnlyList<DbAutomationScalarFunctionCall> FindScalarFunctionCalls(
        string? expression,
        IEnumerable<string>? ignoredNames = null)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return [];

        HashSet<string> ignored = ignoredNames is null
            ? []
            : new HashSet<string>(ignoredNames, StringComparer.OrdinalIgnoreCase);
        var calls = new List<DbAutomationScalarFunctionCall>();
        ReadOnlySpan<char> input = expression.AsSpan();
        for (int i = 0; i < input.Length; i++)
        {
            char current = input[i];
            if (current is '\'' or '"')
            {
                i = SkipQuoted(input, i, current);
                continue;
            }

            if (current == '[')
            {
                i = SkipBracketed(input, i);
                continue;
            }

            if (!IsIdentifierStart(current))
                continue;

            int start = i;
            i++;
            while (i < input.Length && IsIdentifierPart(input[i]))
                i++;

            string name = input[start..i].ToString();
            int cursor = i;
            while (cursor < input.Length && char.IsWhiteSpace(input[cursor]))
                cursor++;

            if (cursor >= input.Length || input[cursor] != '(')
            {
                i--;
                continue;
            }

            if (!ignored.Contains(name) && TryCountArguments(input, cursor, out int arity))
                calls.Add(new DbAutomationScalarFunctionCall(name, arity));

            i--;
        }

        return calls
            .GroupBy(
                static call => $"{call.Name}|{call.Arity}",
                StringComparer.OrdinalIgnoreCase)
            .Select(static group => group.First())
            .OrderBy(static call => call.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static call => call.Arity)
            .ToArray();
    }

    private static bool TryCountArguments(ReadOnlySpan<char> input, int openParen, out int arity)
    {
        arity = 0;
        int depth = 0;
        bool sawArgument = false;
        bool expectingArgument = true;
        for (int i = openParen; i < input.Length; i++)
        {
            char current = input[i];
            if (current is '\'' or '"')
            {
                i = SkipQuoted(input, i, current);
                sawArgument = true;
                expectingArgument = false;
                continue;
            }

            if (current == '[')
            {
                i = SkipBracketed(input, i);
                sawArgument = true;
                expectingArgument = false;
                continue;
            }

            if (current == '(')
            {
                depth++;
                if (depth > 1)
                {
                    sawArgument = true;
                    expectingArgument = false;
                }

                continue;
            }

            if (current == ')')
            {
                depth--;
                if (depth < 0)
                    return false;

                if (depth == 0)
                {
                    if (expectingArgument && sawArgument)
                        return false;

                    arity = sawArgument ? arity + 1 : 0;
                    return true;
                }

                continue;
            }

            if (current == ',' && depth == 1)
            {
                if (expectingArgument)
                    return false;

                arity++;
                expectingArgument = true;
                continue;
            }

            if (depth == 1 && !char.IsWhiteSpace(current))
            {
                sawArgument = true;
                expectingArgument = false;
            }
        }

        return false;
    }

    private static int SkipQuoted(ReadOnlySpan<char> input, int start, char quote)
    {
        for (int i = start + 1; i < input.Length; i++)
        {
            if (input[i] == quote)
                return i;
        }

        return input.Length - 1;
    }

    private static int SkipBracketed(ReadOnlySpan<char> input, int start)
    {
        for (int i = start + 1; i < input.Length; i++)
        {
            if (input[i] == ']')
                return i;
        }

        return input.Length - 1;
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';

    private static bool IsIdentifierPart(char value)
        => char.IsLetterOrDigit(value) || value == '_';
}
