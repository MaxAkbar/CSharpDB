using CSharpDB.Core;

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

    private static string GetValueColor(DbValue value) => value.Type switch
    {
        DbType.Null => Ansi.Dim + Ansi.Italic,
        DbType.Integer or DbType.Real => Ansi.Yellow,
        DbType.Text => Ansi.Green,
        DbType.Blob => Ansi.Magenta,
        _ => "",
    };
}
