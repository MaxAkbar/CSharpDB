# Trusted C# Host Sample

This sample is the VS Code workflow for CSharpDB's trusted C# integration. It
is a normal C# project: open this folder in VS Code, set breakpoints inside the
registered callbacks, and run or debug the host process.

It demonstrates:

- registering a trusted scalar function with `DatabaseOptions.ConfigureFunctions`
- calling that function from SQL
- registering a trusted command with `DbCommandRegistry`
- running an Admin Forms action sequence that sets a field and calls the command

The sample keeps the important runtime boundary visible: C# callback bodies live
in the host project. The database/form metadata stores names and action data
only.

## Run From VS Code

1. Open `samples/trusted-csharp-host` in VS Code.
2. Install the C# Dev Kit or C# extension if VS Code prompts for it.
3. Press `F5`, or run the task `run trusted C# host sample`.
4. Put breakpoints in `Slugify` or the `AuditCustomerChange` command callback.

## Run From Terminal

```powershell
dotnet run --project samples\trusted-csharp-host\TrustedCSharpHostSample.csproj
```

Expected output includes slug values from SQL and an audit entry from the form
action sequence.

## Files

- `Program.cs` contains the host registration code and runnable demo.
- `.vscode/launch.json` launches the sample under the debugger.
- `.vscode/tasks.json` builds and runs the sample from VS Code tasks.
