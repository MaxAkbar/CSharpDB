using System.Text;
using CSharpDB.EntityFrameworkCore.Tools;

var strictUtf8 = new UTF8Encoding(
    encoderShouldEmitUTF8Identifier: false,
    throwOnInvalidBytes: true);
Console.InputEncoding = strictUtf8;
Console.OutputEncoding = strictUtf8;

using var cancellation = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};
Console.CancelKeyPress += cancelHandler;

try
{
    if (args.Length > 0 &&
        string.Equals(
            args[0],
            "--worker",
            StringComparison.Ordinal))
    {
        Stream protocolInput = Console.OpenStandardInput();
        await using Stream protocolOutputStream =
            Console.OpenStandardOutput();
        await using Stream protocolErrorStream =
            Console.OpenStandardError();
        await using var protocolOutput = new StreamWriter(
            protocolOutputStream,
            strictUtf8,
            bufferSize: 4 * 1024,
            leaveOpen: true)
        {
            AutoFlush = true,
        };
        await using var protocolError = new StreamWriter(
            protocolErrorStream,
            strictUtf8,
            bufferSize: 4 * 1024,
            leaveOpen: true)
        {
            AutoFlush = true,
        };

        // Keep application startup, factories, and migration code from
        // obtaining the protocol writers through Console.Out or Console.Error.
        // The worker writes only through the handles captured above.
        Console.SetOut(TextWriter.Null);
        Console.SetError(TextWriter.Null);

        try
        {
            return await EfCoreWorkerRunner.RunAsync(
                args,
                protocolInput,
                protocolOutput,
                protocolError,
                EfCoreWorkerDependencies.Default,
                cancellation.Token);
        }
        catch (OperationCanceledException)
            when (cancellation.IsCancellationRequested)
        {
            return EfCoreAnalyzeCommandRunner.ExitCanceled;
        }
    }

    try
    {
        return await EfCoreAnalyzeCommandRunner.RunAsync(
            args,
            Console.Out,
            Console.Error,
            cancellation.Token);
    }
    catch (OperationCanceledException)
        when (cancellation.IsCancellationRequested)
    {
        await Console.Error.WriteLineAsync("Canceled.");
        return EfCoreAnalyzeCommandRunner.ExitCanceled;
    }
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
}
