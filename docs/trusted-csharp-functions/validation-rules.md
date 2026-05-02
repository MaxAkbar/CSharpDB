# Trusted Validation Rules

Admin Forms can run host-registered validation callbacks before a record is
saved. The form stores only the validation rule name, fallback message, and JSON
parameters. The C# callback body is compiled into the host application and
registered during startup.

Validation rules are trusted in-process callbacks. They are intended for
business checks that do not belong in generic field metadata, such as
cross-field validation, tenant-specific policies, or checks against host-owned
services.

## Register Rules

Register rules with `AddCSharpDbAdminFormValidationRules(...)`:

```csharp
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

builder.Services.AddCSharpDbAdminFormValidationRules(rules =>
{
    rules.AddRule(
        "CustomerNamePolicy",
        new DbValidationRuleOptions(
            Description: "Rejects placeholder customer names.",
            Timeout: TimeSpan.FromSeconds(2)),
        static (context, ct) =>
        {
            string text = context.Value.IsNull ? string.Empty : context.Value.AsText;
            string blockedText = context.Parameters.TryGetValue("blockedText", out DbValue configured)
                && !configured.IsNull
                    ? configured.AsText
                    : "test";

            DbValidationRuleResult result = text.Contains(blockedText, StringComparison.OrdinalIgnoreCase)
                ? DbValidationRuleResult.Failure(
                    context.FallbackMessage ?? "Customer name is not allowed.",
                    context.FieldName,
                    context.RuleName)
                : DbValidationRuleResult.Success();
            return ValueTask.FromResult(result);
        });
});
```

Rule names are case-insensitive identifiers. Duplicate names fail during
registration so the host fails fast at startup.

## Field-Level Rules

Field-level rules are attached to a bound control through
`ValidationOverride.AddRules`. The runtime context includes the current field
value, field name, control id, full record, parameters, and metadata.

```csharp
new ControlDefinition(
    "customer-name",
    "text",
    new Rect(24, 72, 240, 32),
    new BindingDefinition("Name", "TwoWay"),
    PropertyBag.Empty,
    new ValidationOverride(
        DisableInferredRules: false,
        AddRules:
        [
            new ValidationRule(
                "CustomerNamePolicy",
                "Use the real customer name, not a placeholder.",
                new Dictionary<string, object?>
                {
                    ["blockedText"] = "test",
                }),
        ],
        DisableRuleIds: []));
```

The fallback message is used when the callback returns a failure without a more
specific message. Parameters are converted to `DbValue` and are available through
`context.Parameters`.

## Form-Level Rules

Form-level rules live on `FormDefinition.ValidationRules`. Use them for
cross-field checks and global save policies. A form-level callback can return
field-specific failures or global failures. A failure with `FieldName = null` or
an empty field name is shown as a form-level error.

```csharp
builder.Services.AddCSharpDbAdminFormValidationRules(rules =>
{
    rules.AddRule(
        "CustomerReadyForInsert",
        static context =>
        {
            string status = context.Record.TryGetValue("Status", out DbValue value)
                && !value.IsNull
                    ? value.AsText
                    : string.Empty;

            return string.Equals(status, "Ready", StringComparison.OrdinalIgnoreCase)
                ? DbValidationRuleResult.Success()
                : DbValidationRuleResult.Failure(
                    [
                        new DbValidationFailure(
                            "Status",
                            "Customer status must be Ready before save.",
                            context.RuleName),
                    ],
                    "Customer record is not ready.");
        });
});
```

Attach the rule to the form:

```csharp
var form = existingForm with
{
    ValidationRules =
    [
        new ValidationRule(
            "CustomerReadyForInsert",
            "Customer status must be Ready before save.",
            new Dictionary<string, object?>()),
    ],
};
```

## Runtime Context

Every rule receives `DbValidationRuleContext`:

| Property | Meaning |
| --- | --- |
| `RuleName` | The registered callback name. |
| `Scope` | `Field` or `Form`. |
| `Record` | Full current record as `IReadOnlyDictionary<string, DbValue>`. |
| `Parameters` | JSON parameters from form metadata as `DbValue`s. |
| `Metadata` | Surface, owner, location, correlation id, and form details. |
| `FormId`, `FormName`, `TableName` | Current form source metadata. |
| `ControlId`, `FieldName`, `Value` | Field-level context; null/default for form-level rules. |
| `FallbackMessage` | Designer-provided fallback message. |

Validation callbacks are asynchronous:

```csharp
public delegate ValueTask<DbValidationRuleResult> DbValidationRuleDelegate(
    DbValidationRuleContext context,
    CancellationToken ct);
```

Pass the cancellation token to host I/O. If a rule uses host services, capture
thread-safe services in the registration closure or register the rule from a
host-owned composition root.

## Policy

Validation rules request the `DbExtensionCapability.ValidationRules`
capability. `DbExtensionPolicies.DefaultHostCallbackPolicy` grants validation
rules by default. If the host uses a custom policy, it must grant that
capability:

```csharp
builder.Services.AddSingleton(new DbExtensionPolicy(
    AllowExtensions: true,
    Grants:
    [
        new DbExtensionCapabilityGrant(
            DbExtensionCapability.ValidationRules,
            DbExtensionCapabilityGrantStatus.Granted,
            Exports: ["CustomerNamePolicy", "CustomerReadyForInsert"]),
    ],
    DefaultTimeout: TimeSpan.FromSeconds(5),
    RequireSignature: true,
    AllowedHostModes: DbExtensionHostMode.Embedded));
```

Scoped grants can use `Exports`, `Tables`, and `Scope`. Deny grants take
precedence over allows when both match a callback request.

## Failure Behavior

Validation rules fail closed. Save is blocked when a rule is:

- not registered
- denied by policy
- timed out
- canceled by the validation runtime
- throwing an exception
- returning a failed result

The Admin callbacks tab shows registered validation rules, saved references,
policy decisions, and diagnostics history. Missing references mean saved form
metadata names a rule that the current host has not registered.

## Generated Stubs

Forms export automation metadata for validation rule references. The callback
catalog can generate starter registrations:

```csharp
public static void Register(
    DbFunctionRegistryBuilder functions,
    DbCommandRegistryBuilder commands,
    DbValidationRuleRegistryBuilder validationRules)
{
    validationRules.AddRule(
        "CustomerNamePolicy",
        new DbValidationRuleOptions(
            Description: "TODO: describe validation rule."),
        static async (context, ct) =>
        {
            await ValueTask.CompletedTask;
            return DbValidationRuleResult.Success();
        });
}
```

The generated code is a handoff artifact. Keep the rule implementation in the
host project, not in form JSON or database metadata.

## Runnable Sample

The trusted host sample registers scalar functions, commands, and validation
rules:

```powershell
dotnet run --project samples\trusted-csharp-host\TrustedCSharpHostSample.csproj
```

The sample exports form automation metadata, validates that the referenced
callbacks are registered, prints generated stubs, and runs a validation demo.
