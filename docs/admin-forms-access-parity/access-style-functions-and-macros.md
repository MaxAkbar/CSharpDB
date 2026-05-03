# Access-Style Functions and Macros

This note captures the Access-parity function and macro set we want available
in Admin Forms. The goal is not to clone every Access/VBA surface area at once.
The goal is to include the small, familiar built-in set that makes form
expressions, validation, conditional formatting, buttons, and simple workflows
feel productive without requiring host code for every common task.

## Design Direction

- Keep simple expression functions built into the formula engine.
- Keep user-facing form actions as declarative Admin Form actions where possible.
- Route dangerous or host-specific actions through trusted callbacks, policy, and
  diagnostics.
- Treat database-owned C# code modules as the later `RunCode` target.
- Preserve the existing saved form wire shape by storing action and expression
  settings in metadata/property bags.

Implementation note: Admin Forms formulas now include the expression functions
listed below as built-ins. Macro/action commands remain roadmap items unless
already covered by the existing form action model.

## Included Expression Functions

### Null and Conditional

These should be first because they appear constantly in form defaults,
calculated controls, validation messages, and visibility/enabled rules.

| Function | Purpose | Priority |
| --- | --- | --- |
| `Nz(value, fallback)` | Replace null/empty values with a fallback. | V1 |
| `IsNull(value)` | Test for null. | V1 |
| `IsEmpty(value)` | Test for empty/unset values where applicable. | V1 |
| `IIf(condition, trueValue, falseValue)` | Inline conditional. | V1 |
| `Switch(condition1, value1, ...)` | Multi-branch conditional. | V2 |
| `Choose(index, value1, value2, ...)` | Pick from positional values. | V2 |

### Text

| Function | Purpose | Priority |
| --- | --- | --- |
| `Len(value)` | Text length. | V1 |
| `Left(value, count)` | Left substring. | V1 |
| `Right(value, count)` | Right substring. | V1 |
| `Mid(value, start, count)` | Middle substring. | V1 |
| `Trim(value)` | Trim both ends. | V1 |
| `LTrim(value)` | Trim left. | V2 |
| `RTrim(value)` | Trim right. | V2 |
| `UCase(value)` | Uppercase text. | V1 |
| `LCase(value)` | Lowercase text. | V1 |
| `InStr(value, search)` | Find substring position. | V1 |
| `Replace(value, search, replacement)` | Replace text. | V1 |
| `StrComp(left, right, comparison)` | Compare strings. | V2 |
| `Val(value)` | Parse leading numeric text. | V2 |

### Date and Time

| Function | Purpose | Priority |
| --- | --- | --- |
| `Date()` | Current date. | V1 |
| `Time()` | Current time. | V1 |
| `Now()` | Current date/time. | V1 |
| `Year(value)` | Extract year. | V1 |
| `Month(value)` | Extract month number. | V1 |
| `Day(value)` | Extract day of month. | V1 |
| `Hour(value)` | Extract hour. | V2 |
| `Minute(value)` | Extract minute. | V2 |
| `Second(value)` | Extract second. | V2 |
| `DateAdd(interval, amount, value)` | Add date/time interval. | V1 |
| `DateDiff(interval, start, end)` | Difference between dates. | V1 |
| `DatePart(interval, value)` | Extract date/time part. | V2 |
| `DateSerial(year, month, day)` | Construct date. | V2 |
| `TimeSerial(hour, minute, second)` | Construct time. | V2 |
| `Weekday(value)` | Day of week number. | V2 |
| `MonthName(month)` | Month display name. | V2 |

### Number and Conversion

| Function | Purpose | Priority |
| --- | --- | --- |
| `Abs(value)` | Absolute value. | V1 |
| `Round(value, digits)` | Round number. | V1 |
| `Int(value)` | Floor-like integer conversion. | V1 |
| `Fix(value)` | Truncate toward zero. | V2 |
| `Sgn(value)` | Sign of number. | V2 |
| `CStr(value)` | Convert to string. | V1 |
| `CInt(value)` | Convert to integer. | V1 |
| `CLng(value)` | Convert to long integer. | V2 |
| `CDbl(value)` | Convert to double. | V1 |
| `CBool(value)` | Convert to boolean. | V1 |
| `CDate(value)` | Convert to date/time. | V1 |
| `Format(value, format)` | Format date/number/text. | V2 |

### Domain Aggregates

Domain aggregate functions are important for Access familiarity, but they need a
careful implementation because they read other rows/tables during form
evaluation.

| Function | Purpose | Priority |
| --- | --- | --- |
| `DLookup(expr, domain, criteria)` | Read one value from a table/query. | V2 |
| `DCount(expr, domain, criteria)` | Count matching rows. | V2 |
| `DSum(expr, domain, criteria)` | Sum matching rows. | V2 |
| `DAvg(expr, domain, criteria)` | Average matching rows. | V2 |
| `DMin(expr, domain, criteria)` | Minimum matching value. | V2 |
| `DMax(expr, domain, criteria)` | Maximum matching value. | V2 |

V2 should enforce query/table access through the same callback and diagnostics
boundary used for trusted extensions where relevant. These functions should also
have row limits and clear error handling so formula evaluation cannot become an
unbounded database workload.

## Included Macro and Action Commands

### Record Actions

These map cleanly to existing form runtime behavior and should remain
declarative actions rather than host callbacks.

| Action | Purpose | Priority |
| --- | --- | --- |
| `NewRecord` | Start a new record. | V1 |
| `SaveRecord` | Save the current record. | V1 |
| `DeleteRecord` | Delete the current record. | V1 |
| `UndoRecord` | Revert unsaved edits. | V1 |
| `RefreshRecord` | Reload the current record. | V1 |
| `Requery` | Reload the form record source. | V1 |
| `GoToRecord` | Navigate to a specific record. | V1 |
| `FindRecord` | Search/navigate by criteria. | V2 |
| `NextRecord` | Navigate forward. | V1 |
| `PreviousRecord` | Navigate backward. | V1 |

### Form, Window, and Report Actions

| Action | Purpose | Priority |
| --- | --- | --- |
| `OpenForm` | Open another saved form. | V1 |
| `CloseForm` | Close current or named form. | V1 |
| `OpenReport` | Open a saved report. | V2 |
| `CloseReport` | Close a report surface. | V2 |
| `PreviewReport` | Open report preview. | V2 |
| `PrintReport` | Print or export through report pipeline. | V2 |

### Filter and Sort Actions

| Action | Purpose | Priority |
| --- | --- | --- |
| `ApplyFilter` | Apply a form filter. | V1 |
| `ClearFilter` | Clear current filter. | V1 |
| `SetOrderBy` | Apply sort order. | V1 |
| `ClearOrderBy` | Clear sort order. | V1 |
| `SearchRecords` | Search over configured searchable fields. | V2 |

### UI and Control Actions

These are needed for Access-style command buttons and conditional workflows.

| Action | Purpose | Priority |
| --- | --- | --- |
| `SetValue` | Set a field/control value. | V1 |
| `SetProperty` | Set visible/enabled/read-only/text/style properties. | V1 |
| `SetFocus` | Move focus to a control. | V1 |
| `EnableControl` | Set enabled state. | V1 |
| `DisableControl` | Clear enabled state. | V1 |
| `ShowControl` | Set visible state. | V1 |
| `HideControl` | Clear visible state. | V1 |
| `LockControl` | Set read-only state. | V1 |
| `UnlockControl` | Clear read-only state. | V1 |
| `MsgBox` | Show a message dialog. | V1 |
| `InputBox` | Prompt for a value. | V2 |

### Flow Actions

| Action | Purpose | Priority |
| --- | --- | --- |
| `If` / `Else` | Conditional action branching. | V1 |
| `StopMacro` | Stop the current action sequence. | V1 |
| `RunMacro` | Run a named action sequence. | V1 |
| `RunActionSequence` | Existing explicit reusable sequence action. | V1 |
| `OnError` | Configure failure handling. | V2 |

### Data and Query Actions

These must be gated carefully because they can mutate data outside the current
form record.

| Action | Purpose | Priority |
| --- | --- | --- |
| `OpenQuery` | Open a saved query result. | V2 |
| `RunQuery` | Execute a saved query. | V2 trusted |
| `RunProcedure` | Execute a saved procedure. | V2 trusted |
| `RunSQL` | Execute SQL text. | Later trusted |
| `ExportData` | Export records. | Later trusted |
| `ImportData` | Import records. | Later trusted |

### Code Bridge Actions

| Action | Purpose | Priority |
| --- | --- | --- |
| `RunCommand` | Invoke host-registered trusted command callback. | Existing |
| `RunCode` | Invoke database-owned C# code module function. | Later |

`RunCode` should wait for the database code modules work. It should compile and
execute database-owned C# through a trusted build/runtime model, not arbitrary
source text embedded directly inside form JSON.

### Temp and Session Variables

| Action | Purpose | Priority |
| --- | --- | --- |
| `SetTempVar` | Store a session-scoped value. | V2 |
| `RemoveTempVar` | Remove one session value. | V2 |
| `RemoveAllTempVars` | Clear session values. | V2 |

Temp variables should be scoped to the current user/session and be available to
form expressions, filters, and action sequences.

## Recommended Implementation Order

1. Add core formula functions: `Nz`, `IIf`, `Date`, `Now`, common text helpers,
   numeric helpers, and conversions.
2. Add declarative form actions: `MsgBox`, `SetValue`, `SetProperty`,
   `SetFocus`, `ApplyFilter`, `ClearFilter`, `Requery`, `OpenForm`, and
   `CloseForm`.
3. Add domain aggregates with clear access checks, row limits, and diagnostics.
4. Add temp/session variables and make them visible to expressions and actions.
5. Add `RunCode` after database code modules exist.
6. Add import/export/file/app-launch style actions only as trusted operations.

## Notes on Access Compatibility

Microsoft Access exposes many functions and macro commands. CSharpDB should use
the familiar names where it makes sense, but implementation should follow the
CSharpDB security and diagnostics model. Anything that can call host code,
modify unrelated data, access files, or run arbitrary SQL should go through an
explicit trusted boundary.

Useful Microsoft references:

- [Access functions by category](https://support.microsoft.com/en-us/office/access-functions-by-category-b8b136c3-2716-4d39-94a2-658ce330ed83)
- [Introduction to macros](https://support.microsoft.com/en-gb/office/introduction-to-macros-a39c2a26-e745-4957-8d06-89e0b435aac3)
- [Access macro commands](https://learn.microsoft.com/en-ie/office/client-developer/access/desktop-database-reference/macro-commands)
