using System.Globalization;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Cli;

/// <summary>
/// Formats query results as a Unicode box-drawing table with ANSI colors.
/// </summary>
internal sealed class TableFormatter
{
    private readonly TextWriter _out;

    public TableFormatter(TextWriter output)
    {
        _out = output;
    }

    public void PrintTable(ColumnDefinition[] schema, List<DbValue[]> rows)
    {
        int colCount = schema.Length;
        if (colCount == 0) return;

        // Calculate column widths (max of header name and all cell values).
        var widths = new int[colCount];
        for (int c = 0; c < colCount; c++)
            widths[c] = schema[c].Name.Length;

        foreach (var row in rows)
        {
            for (int c = 0; c < colCount; c++)
            {
                int len = FormatValue(row[c]).Length;
                if (len > widths[c]) widths[c] = len;
            }
        }

        // Cap column widths to a reasonable maximum.
        const int maxWidth = 40;
        for (int c = 0; c < colCount; c++)
            widths[c] = Math.Min(widths[c], maxWidth);

        string border = Ansi.Dim + Ansi.BrightBlack;

        // Top border: ┌────┬────┐
        PrintBorderLine(widths, border, '┌', '┬', '┐');

        // Header row
        _out.Write(border + "│" + Ansi.Reset);
        for (int c = 0; c < colCount; c++)
        {
            string header = schema[c].Name.PadRight(widths[c]);
            _out.Write(" " + Ansi.Bold + Ansi.White + header + Ansi.Reset + " ");
            _out.Write(border + "│" + Ansi.Reset);
        }
        _out.WriteLine();

        // Header separator: ├────┼────┤
        PrintBorderLine(widths, border, '├', '┼', '┤');

        // Data rows
        foreach (var row in rows)
        {
            _out.Write(border + "│" + Ansi.Reset);
            for (int c = 0; c < colCount; c++)
            {
                string text = FormatValue(row[c]);
                if (text.Length > widths[c])
                    text = text[..(widths[c] - 1)] + "…";

                string padded = row[c].Type is DbType.Integer or DbType.Real
                    ? text.PadLeft(widths[c])
                    : text.PadRight(widths[c]);

                string color = GetValueColor(row[c]);
                _out.Write(" " + color + padded + Ansi.Reset + " ");
                _out.Write(border + "│" + Ansi.Reset);
            }
            _out.WriteLine();
        }

        // Bottom border: └────┴────┘
        PrintBorderLine(widths, border, '└', '┴', '┘');
    }

    public void PrintTable(string[] columnNames, List<object?[]> rows)
    {
        int colCount = columnNames.Length;
        if (colCount == 0) return;

        var widths = new int[colCount];
        for (int c = 0; c < colCount; c++)
            widths[c] = columnNames[c].Length;

        foreach (var row in rows)
        {
            for (int c = 0; c < colCount && c < row.Length; c++)
            {
                int len = FormatObjectValue(row[c]).Length;
                if (len > widths[c]) widths[c] = len;
            }
        }

        const int maxWidth = 40;
        for (int c = 0; c < colCount; c++)
            widths[c] = Math.Min(widths[c], maxWidth);

        string border = Ansi.Dim + Ansi.BrightBlack;

        PrintBorderLine(widths, border, '┌', '┬', '┐');

        _out.Write(border + "│" + Ansi.Reset);
        for (int c = 0; c < colCount; c++)
        {
            string header = columnNames[c].PadRight(widths[c]);
            _out.Write(" " + Ansi.Bold + Ansi.White + header + Ansi.Reset + " ");
            _out.Write(border + "│" + Ansi.Reset);
        }
        _out.WriteLine();

        PrintBorderLine(widths, border, '├', '┼', '┤');

        foreach (var row in rows)
        {
            _out.Write(border + "│" + Ansi.Reset);
            for (int c = 0; c < colCount; c++)
            {
                object? value = c < row.Length ? row[c] : null;
                string text = FormatObjectValue(value);
                if (text.Length > widths[c])
                    text = text[..(widths[c] - 1)] + "…";

                string padded = IsNumericValue(value)
                    ? text.PadLeft(widths[c])
                    : text.PadRight(widths[c]);

                string color = GetValueColor(value);
                _out.Write(" " + color + padded + Ansi.Reset + " ");
                _out.Write(border + "│" + Ansi.Reset);
            }
            _out.WriteLine();
        }

        PrintBorderLine(widths, border, '└', '┴', '┘');
    }

    private void PrintBorderLine(int[] widths, string border, char left, char mid, char right)
    {
        _out.Write(border);
        _out.Write(left);
        for (int c = 0; c < widths.Length; c++)
        {
            _out.Write(new string('─', widths[c] + 2));
            _out.Write(c < widths.Length - 1 ? mid : right);
        }
        _out.Write(Ansi.Reset);
        _out.WriteLine();
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

    private static string GetValueColor(DbValue value) => value.Type switch
    {
        DbType.Null => Ansi.Dim + Ansi.Italic,
        DbType.Integer or DbType.Real => Ansi.Yellow,
        DbType.Text => Ansi.Green,
        DbType.Blob => Ansi.Magenta,
        _ => "",
    };

    private static string GetValueColor(object? value) => value switch
    {
        null => Ansi.Dim + Ansi.Italic,
        byte[] => Ansi.Magenta,
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal => Ansi.Yellow,
        string => Ansi.Green,
        JsonElement element => element.ValueKind switch
        {
            JsonValueKind.Null or JsonValueKind.Undefined => Ansi.Dim + Ansi.Italic,
            JsonValueKind.Number => Ansi.Yellow,
            JsonValueKind.String => Ansi.Green,
            _ => "",
        },
        _ => "",
    };

    private static bool IsNumericValue(object? value) => value switch
    {
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal => true,
        JsonElement element when element.ValueKind == JsonValueKind.Number => true,
        _ => false,
    };
}
