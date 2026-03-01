namespace CSharpDB.Cli;

/// <summary>
/// Lightweight ANSI escape-code helpers for terminal output.
/// </summary>
internal static class Ansi
{
    private const string Esc = "\x1b[";

    // Reset
    public const string Reset = $"{Esc}0m";

    // Styles
    public const string Bold = $"{Esc}1m";
    public const string Dim = $"{Esc}2m";
    public const string Italic = $"{Esc}3m";

    // Foreground colors
    public const string Black = $"{Esc}30m";
    public const string Red = $"{Esc}31m";
    public const string Green = $"{Esc}32m";
    public const string Yellow = $"{Esc}33m";
    public const string Blue = $"{Esc}34m";
    public const string Magenta = $"{Esc}35m";
    public const string Cyan = $"{Esc}36m";
    public const string White = $"{Esc}37m";

    // Bright foreground colors
    public const string BrightBlack = $"{Esc}90m";
    public const string BrightRed = $"{Esc}91m";
    public const string BrightGreen = $"{Esc}92m";
    public const string BrightYellow = $"{Esc}93m";
    public const string BrightCyan = $"{Esc}96m";
    public const string BrightWhite = $"{Esc}97m";

    public static string Colorize(string text, string color) => $"{color}{text}{Reset}";

    public static void EnableVirtualTerminal()
    {
        // On Windows, ensure the console supports ANSI escape codes.
        // .NET 9 on modern Windows Terminal handles this automatically,
        // but we set output encoding to UTF-8 for box-drawing characters.
        Console.OutputEncoding = System.Text.Encoding.UTF8;
    }
}
