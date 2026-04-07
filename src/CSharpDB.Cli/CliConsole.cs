using System.Reflection;
using System.Text;
using Spectre.Console;

namespace CSharpDB.Cli;

internal static class CliConsole
{
    private static readonly string Version = GetVersion();

    public static void ConfigureTerminal()
    {
        Console.OutputEncoding = Encoding.UTF8;
    }

    public static IAnsiConsole Create(TextWriter output, bool interactive = false)
    {
        if (ReferenceEquals(output, Console.Out) && !Console.IsOutputRedirected)
            return Spectre.Console.AnsiConsole.Console;

        return Spectre.Console.AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.Yes,
            ColorSystem = ColorSystemSupport.TrueColor,
            Interactive = interactive ? InteractionSupport.Yes : InteractionSupport.No,
            Out = new AnsiConsoleOutput(output),
        });
    }

    public static string Escape(string? value) => Markup.Escape(value ?? string.Empty);

    public static void WriteBanner(IAnsiConsole console, string databasePath)
    {
        console.WriteLine();

        var figlet = new FigletText("CSharpDB")
            .Color(Color.DeepSkyBlue1);
        console.Write(figlet);

        var grid = new Grid();
        grid.AddColumn(new GridColumn().NoWrap().PadRight(2));
        grid.AddColumn(new GridColumn());
        grid.AddRow(
            new Markup("[grey]Version:[/]"),
            new Markup($"[deepskyblue1]{Escape(Version)}[/]"));
        grid.AddRow(
            new Markup("[grey]Database:[/]"),
            new Markup($"[white]{Escape(databasePath)}[/]"));
        grid.AddRow(
            new Markup("[grey]Help:[/]"),
            new Markup("[deepskyblue1].help[/] [grey]for commands,[/] [deepskyblue1].quit[/] [grey]to exit[/]"));

        var panel = new Panel(grid)
            .RoundedBorder()
            .BorderColor(Color.Grey42)
            .Padding(1, 0);

        console.Write(panel);
        console.WriteLine();
    }

    public static void WritePrompt(IAnsiConsole console, bool hasPendingSql)
    {
        if (hasPendingSql)
        {
            console.Markup("[grey]  ...> [/]");
            return;
        }

        console.Markup("[deepskyblue1]csdb[/] [grey]>[/] ");
    }

    public static void WriteError(IAnsiConsole console, string message)
        => console.MarkupLine($"[bold red]Error:[/] {Escape(message)}");

    public static void WriteWarning(IAnsiConsole console, string message)
        => console.MarkupLine($"[bold yellow]Warning:[/] {Escape(message)}");

    public static void WriteSuccess(IAnsiConsole console, string message)
        => console.MarkupLine($"[bold green]{Escape(message)}[/]");

    public static void WriteMuted(IAnsiConsole console, string message)
        => console.MarkupLine($"[grey]{Escape(message)}[/]");

    public static Table CreateKeyValueTable()
    {
        return new Table()
            .Border(TableBorder.None)
            .HideHeaders();
    }

    public static Table CreateDataTable()
    {
        return new Table()
            .RoundedBorder()
            .BorderColor(Color.Grey42);
    }

    public static void WriteNameList(IAnsiConsole console, IEnumerable<string> names, string emptyMessage)
    {
        var ordered = names.ToArray();
        if (ordered.Length == 0)
        {
            WriteMuted(console, emptyMessage);
            return;
        }

        var table = new Table()
            .Border(TableBorder.None)
            .HideHeaders();
        table.AddColumn(new TableColumn(string.Empty));

        foreach (string name in ordered)
            table.AddRow(new Markup($"[deepskyblue1]{Escape(name)}[/]"));

        console.Write(table);
    }

    public static void WriteSqlPanel(IAnsiConsole console, string title, string sql)
    {
        var panel = new Panel(new Text(sql))
            .RoundedBorder()
            .BorderColor(Color.Grey42)
            .Header($"[bold deepskyblue1]{Escape(title)}[/]", Justify.Left)
            .Padding(1, 0);
        console.Write(panel);
    }

    private static string GetVersion()
    {
        var assembly = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
        var infoVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
        if (infoVersion is not null)
        {
            string version = infoVersion.InformationalVersion;
            int plusIndex = version.IndexOf('+');
            return plusIndex >= 0 ? version[..plusIndex] : version;
        }

        return assembly.GetName().Version?.ToString(3) ?? "0.0.0";
    }
}
