using System.Text;
using Spectre.Console;

namespace CSharpDB.Cli;

internal static class CliConsole
{
    private const int FullLogoWidth = 80;
    private const string BannerLogo = """
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@%%%########%%%@@@@@@@@@@@@@@@@@@%***#%@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@%*+=-:.....      .....::-+*%@@@@@@@%#*+++++++*#%@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@*:.     ................     .=@@@%**++++++++++++++*#%@@@@@@@@@@@@@@@@@
@@@@@@@@@-   ...................   .-+###*+++++++++++++++++++++**%@@@@@@@@@@@@@@
@@@@@@@@@#*=-..                 :=*#%#*++++++++++++++++++++++++++++*#%@@@@@@@@@@
@@@@@@@@@:-+*##**+==---:::::---*%#*+++++++++**###%###*++++++++++++++++*#@@@@@@@@
@@@@@@@@@-..:.:-==+*##########@@+++++++++*#%@@@@@@@@@@@%#+++++++++++**##%@@@@@@@
@@@@@@@@@-:::...   .:::::::::.#@++++++++#@@@@@@@@@@@@@@@@*++++++***######@@@@@@@
@@@@@@@@@-:::.......:::::::::.#@+++++++%@@@@@@%#***#%@@%*++*@@*#@@#######@@@@@@@
@@@@@@@@@#=--:......:::::::::.#@++++++%@@@@@%*++++++++*++#%@@@@@@@@@#####@@@@@@@
@@@@@@@@@*%%#*+==-------------#@+++++*@@@@@@++++++++++***%%@@%%@@@%%#####@@@@@@@
@@@@@@@@@::=*##%%%%#######*###%%+++++*@@@@@@+++++++**####%%@@%%@@@%######@@@@@@@
@@@@@@@@@-.....::-=++********+%%++++++%@@@@@#++***######%@@@@@@@@@@%####%@@@@@@@
@@@@@@@@@:.::..... ...........#@++++++*@@@@@@%%######%%###%@@##@@#######%@@@@@@@
@@@@@@@@@=:::.......:::::::::.#@+++++++*@@@@@@@@@@@@@@@@%###############%@@@@@@@
@@@@@@@@@%*+=-::....:::::::::.#@+++++***#%@@@@@@@@@@@@@@@%###############@@@@@@@
@@@@@@@@@+#%%##*++============#@++**########%%@@@@@@@%%#################%@@@@@@@
@@@@@@@@@:.:=+*##%%%%%%%%%%%%%%@%%#####################################%@@@@@@@@
@@@@@@@@@-:::. ...:---======---:-+#%%%#############################%@@@@@@@@@@@@
@@@@@@@@@-.::.......:::.....:::::.::=*%%%#######################%@@@@@@@@@@@@@@@
@@@@@@@@@#-:.. .....:::::::::::::::...:=@@@%################%%@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@%*+-::...::......::::::-=+*%@@@@@@@%%#########%@@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@%%%##########%%%@@@@@@@@@@@@@@@@@%%#%%@@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
""";

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
        WriteLogo(console);

        var content = new Rows(
        [
            new Markup("[bold deepskyblue1]CSharpDB[/] [grey]- Interactive SQL Shell[/]"),
            new Markup($"[grey]Database:[/] {Escape(databasePath)}"),
            new Markup("[grey]Type[/] [aqua].help[/] [grey]for commands,[/] [aqua].quit[/] [grey]to exit.[/]"),
        ]);

        var panel = new Panel(content)
            .RoundedBorder()
            .BorderColor(Color.Grey)
            .Header("[bold]Session[/]", Justify.Left);

        console.Write(panel);
        console.WriteLine();
    }

    private static void WriteLogo(IAnsiConsole console)
    {
        if (console.Profile.Width >= FullLogoWidth)
        {
            var logo = new Text(BannerLogo.TrimEnd(), new Style(foreground: Color.Grey70));
            console.Write(logo);
            console.WriteLine();
            return;
        }

        console.Write(new Rule("[bold deepskyblue1]CSharpDB[/]").RuleStyle("grey"));
    }

    public static void WritePrompt(IAnsiConsole console, bool hasPendingSql)
    {
        if (hasPendingSql)
        {
            console.Markup("[grey]...> [/]"); 
            return;
        }

        console.Markup("[deepskyblue1]csdb[/][grey]> [/]"); 
    }

    public static void WriteError(IAnsiConsole console, string message)
        => console.MarkupLine($"[bold red]Error:[/] {Escape(message)}");

    public static void WriteWarning(IAnsiConsole console, string message)
        => console.MarkupLine($"[bold yellow]Warning:[/] {Escape(message)}");

    public static void WriteSuccess(IAnsiConsole console, string message)
        => console.MarkupLine($"[green]{Escape(message)}[/]");

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
            .BorderColor(Color.Grey);
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
            .BorderColor(Color.Grey)
            .Header($"[bold]{Escape(title)}[/]", Justify.Left);
        console.Write(panel);
    }
}
