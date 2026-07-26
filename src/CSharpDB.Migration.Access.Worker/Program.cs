using System.Text;
using CSharpDB.Migration.Access.Worker;

Console.OutputEncoding = new UTF8Encoding(
    encoderShouldEmitUTF8Identifier: false,
    throwOnInvalidBytes: true);
Console.InputEncoding = new UTF8Encoding(
    encoderShouldEmitUTF8Identifier: false,
    throwOnInvalidBytes: true);

using var cancellation = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};
Console.CancelKeyPress += cancelHandler;
try
{
    return await AccessWorkerRunner.RunAsync(
        args,
        Console.Out,
        Console.Error,
        AccessWorkerDependencies.Default,
        cancellation.Token);
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
}
