using System.Globalization;
using System.Text.Json;
using CSharpDB.Primitives;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace CSharpDB.Cli;

/// <summary>
/// Formats query results using Spectre.Console tables.
/// </summary>
internal sealed class TableFormatter
{
    private const int MaxCellWidth = 60;
    private readonly IAnsiConsole _console;

    public TableFormatter(IAnsiConsole console)
    {
        _console = console;
    }

    public void PrintTable(ColumnDefinition[] schema, List<DbValue[]> rows)
    {
        if (schema.Length == 0)
            return;

        var table = CliConsole.CreateDataTable();
        foreach (var column in schema)
            table.AddColumn(new TableColumn($"[bold]{CliConsole.Escape(column.Name)}[/]"));

        foreach (var row in rows)
        {
            var cells = new IRenderable[schema.Length];
            for (int i = 0; i < schema.Length; i++)
            {
                DbValue value = row[i];
                string text = Truncate(FormatValue(value));
                string? style = GetValueStyle(value);
                cells[i] = CreateCell(text, style, value.Type is DbType.Integer or DbType.Real);
            }

            table.AddRow(cells);
        }

        _console.Write(table);
    }

    public void PrintTable(string[] columnNames, List<object?[]> rows)
    {
        if (columnNames.Length == 0)
            return;

        var table = CliConsole.CreateDataTable();
        foreach (string columnName in columnNames)
            table.AddColumn(new TableColumn($"[bold]{CliConsole.Escape(columnName)}[/]"));

        foreach (var row in rows)
        {
            var cells = new IRenderable[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                object? value = i < row.Length ? row[i] : null;
                string text = Truncate(FormatObjectValue(value));
                string? style = GetValueStyle(value);
                cells[i] = CreateCell(text, style, IsNumericValue(value));
            }

            table.AddRow(cells);
        }

        _console.Write(table);
    }

    private static IRenderable CreateCell(string text, string? style, bool rightAligned)
    {
        string markup = string.IsNullOrWhiteSpace(style)
            ? CliConsole.Escape(text)
            : $"[{style}]{CliConsole.Escape(text)}[/]";
        IRenderable cell = new Markup(markup);
        return rightAligned ? Align.Right(cell) : cell;
    }

    private static string Truncate(string text)
    {
        if (text.Length <= MaxCellWidth)
            return text;

        return text[..(MaxCellWidth - 1)] + "…";
    }

    private static string FormatValue(DbValue value) => value.ToString();

    private static string FormatObjectValue(object? value) => value switch
    {
        null => "NULL",
        byte[] bytes => $"BLOB({bytes.Length} bytes)",
        JsonElement element => element.ValueKind == JsonValueKind.String
            ? element.GetString() ?? string.Empty
            : element.GetRawText(),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
        _ => value.ToString() ?? string.Empty,
    };

    private static string? GetValueStyle(DbValue value) => value.Type switch
    {
        DbType.Null => "italic grey",
        DbType.Integer or DbType.Real => "yellow",
        DbType.Text => "green",
        DbType.Blob => "orchid",
        _ => null,
    };

    private static string? GetValueStyle(object? value) => value switch
    {
        null => "italic grey",
        byte[] => "orchid",
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal => "yellow",
        string => "green",
        JsonElement element => element.ValueKind switch
        {
            JsonValueKind.Null or JsonValueKind.Undefined => "italic grey",
            JsonValueKind.Number => "yellow",
            JsonValueKind.String => "green",
            _ => null,
        },
        _ => null,
    };

    private static bool IsNumericValue(object? value) => value switch
    {
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal => true,
        JsonElement element when element.ValueKind == JsonValueKind.Number => true,
        _ => false,
    };
}
