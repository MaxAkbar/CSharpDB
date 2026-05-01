# Trusted C# Host Sample

This sample is the VS Code workflow for CSharpDB's trusted C# integration. It
is a normal C# project: open this folder in VS Code, set breakpoints inside the
registered callbacks, and run or debug the host process.

It demonstrates:

- registering a trusted scalar function with `DbFunctionRegistry`
- calling that function from SQL
- registering a trusted command with `DbCommandRegistry`
- exporting Admin Forms automation metadata
- validating automation metadata against registered callbacks
- generating starter C# registration stubs from automation metadata
- running an Admin Forms action sequence that sets a field
- invoking a reusable named Admin Forms action sequence that calls the command
- inspecting an Access-style macro form manifest with open form, filter,
  run SQL, and conditional UI rule actions
- inspecting callback arguments and metadata in console output

The sample keeps the important runtime boundary visible: C# callback bodies live
in the host project. The database/form metadata stores names and action data
only.

## Run From VS Code

1. Open `samples/trusted-csharp-host` in VS Code.
2. Install the C# Dev Kit or C# extension if VS Code prompts for it.
3. Press `F5`, or run the task `run trusted C# host sample`.
4. Watch the sample print exported automation metadata.
5. Watch validation confirm that referenced callbacks are registered.
6. Inspect the generated starter C# registration stub.
7. Put breakpoints in `Slugify` or the `AuditCustomerChange` command callback.

## Developer Handoff Story

The intended production workflow has two roles:

1. An app builder creates metadata in Admin, such as a calculated expression
   that calls `Slugify(...)` or a form action sequence that runs
   `AuditCustomerChange`.
2. A host developer owns the C# implementation, registers that callback during
   startup, and debugs it from VS Code.

The database/form metadata stores callback names, argument values, and
reference locations. It does not store C# source or compiled assemblies.

`Program.cs` shows both registration paths:

```csharp
builder.AddScalar(
    "Slugify",
    arity: 1,
    options: new DbScalarFunctionOptions(
        ReturnType: DbType.Text,
        IsDeterministic: true,
        NullPropagating: true),
    invoke: static (_, args) => DbValue.FromText(Slugify(args[0].AsText)));
```

```csharp
builder.AddCommand(
    "AuditCustomerChange",
    new DbCommandOptions("Records a customer workflow event."),
    context =>
    {
        long customerId = context.Arguments["Id"].AsInteger;
        string status = context.Arguments["Status"].AsText;

        return DbCommandResult.Success(
            $"Customer {customerId} changed to {status}.");
    });
```

When Admin reports a missing callback, use the generated stub as the handoff
artifact. The host developer pastes the registration shape into the host app,
replaces the stub body with reviewed C# code, sets a breakpoint, then runs the
host with `F5` to verify the metadata reference reaches the callback.

## Run From Terminal

```powershell
dotnet run --project samples\trusted-csharp-host\TrustedCSharpHostSample.csproj
```

Expected output includes:

- exported callback metadata and locations
- validation status
- generated starter registration code
- slug values from SQL
- an audit entry from the reusable form action sequence

The audit entry prints callback metadata such as the form event and reusable
action sequence name, along with callback arguments passed from the form record
and action sequence.

## Files

- `Program.cs` contains the host registration code, metadata validation, stub
  generation, and runnable demo.
- `access-style-macro-form.json` contains a Phase 8 form manifest with richer
  macro actions and conditional UI rules.
- `.vscode/launch.json` launches the sample under the debugger.
- `.vscode/tasks.json` builds and runs the sample from VS Code tasks.
