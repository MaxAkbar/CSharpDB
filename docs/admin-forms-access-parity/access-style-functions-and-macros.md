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
- Treat database-owned C# code modules as trusted event handlers first; a
  declarative `RunCode` macro action can build on that runtime later.
- Preserve the existing saved form wire shape by storing action and expression
  settings in metadata/property bags.

Implementation note: Admin Forms formulas now include the expression functions
listed below as built-ins. The rendered Forms runtime also supports the current
declarative action model shown below; entries marked future remain roadmap
items.

## Included Expression Functions

### Null and Conditional

These should be first because they appear constantly in form defaults,
calculated controls, validation messages, and visibility/enabled rules.

| Function | Purpose | Status |
| --- | --- | --- |
| `Nz(value, fallback)` | Replace null/empty values with a fallback. | Shipped |
| `IsNull(value)` | Test for null. | Shipped |
| `IsEmpty(value)` | Test for empty/unset values where applicable. | Shipped |
| `IIf(condition, trueValue, falseValue)` | Inline conditional. | Shipped |
| `Switch(condition1, value1, ...)` | Multi-branch conditional. | Shipped |
| `Choose(index, value1, value2, ...)` | Pick from positional values. | Shipped |

### Text

| Function | Purpose | Status |
| --- | --- | --- |
| `Len(value)` | Text length. | Shipped |
| `Left(value, count)` | Left substring. | Shipped |
| `Right(value, count)` | Right substring. | Shipped |
| `Mid(value, start, count)` | Middle substring. | Shipped |
| `Trim(value)` | Trim both ends. | Shipped |
| `LTrim(value)` | Trim left. | Shipped |
| `RTrim(value)` | Trim right. | Shipped |
| `UCase(value)` | Uppercase text. | Shipped |
| `LCase(value)` | Lowercase text. | Shipped |
| `InStr(value, search)` | Find substring position. | Shipped |
| `Replace(value, search, replacement)` | Replace text. | Shipped |
| `StrComp(left, right, comparison)` | Compare strings. | Shipped |
| `Val(value)` | Parse leading numeric text. | Shipped |

### Date and Time

| Function | Purpose | Status |
| --- | --- | --- |
| `Date()` | Current date. | Shipped |
| `Time()` | Current time. | Shipped |
| `Now()` | Current date/time. | Shipped |
| `Year(value)` | Extract year. | Shipped |
| `Month(value)` | Extract month number. | Shipped |
| `Day(value)` | Extract day of month. | Shipped |
| `Hour(value)` | Extract hour. | Shipped |
| `Minute(value)` | Extract minute. | Shipped |
| `Second(value)` | Extract second. | Shipped |
| `DateAdd(interval, amount, value)` | Add date/time interval. | Shipped |
| `DateDiff(interval, start, end)` | Difference between dates. | Shipped |
| `DatePart(interval, value)` | Extract date/time part. | Shipped |
| `DateSerial(year, month, day)` | Construct date. | Shipped |
| `TimeSerial(hour, minute, second)` | Construct time. | Shipped |
| `Weekday(value)` | Day of week number. | Shipped |
| `MonthName(month)` | Month display name. | Shipped |

### Number and Conversion

| Function | Purpose | Status |
| --- | --- | --- |
| `Abs(value)` | Absolute value. | Shipped |
| `Round(value, digits)` | Round number. | Shipped |
| `Int(value)` | Floor-like integer conversion. | Shipped |
| `Fix(value)` | Truncate toward zero. | Shipped |
| `Sgn(value)` | Sign of number. | Shipped |
| `CStr(value)` | Convert to string. | Shipped |
| `CInt(value)` | Convert to integer. | Shipped |
| `CLng(value)` | Convert to long integer. | Shipped |
| `CDbl(value)` | Convert to double. | Shipped |
| `CBool(value)` | Convert to boolean. | Shipped |
| `CDate(value)` | Convert to date/time. | Shipped |
| `Format(value, format)` | Format date/number/text. | Shipped |

### Domain Aggregates

Domain aggregate functions are important for Access familiarity. They are
available in Admin Forms formulas through the rendered Forms runtime, which
loads referenced domains with a row limit and evaluates criteria through the
Forms filter parser.

| Function | Purpose | Status |
| --- | --- | --- |
| `DLookup(expr, domain, criteria)` | Read one value from a table/query. | Shipped in Admin Forms |
| `DCount(expr, domain, criteria)` | Count matching rows. | Shipped in Admin Forms |
| `DSum(expr, domain, criteria)` | Sum matching rows. | Shipped in Admin Forms |
| `DAvg(expr, domain, criteria)` | Average matching rows. | Shipped in Admin Forms |
| `DMin(expr, domain, criteria)` | Minimum matching value. | Shipped in Admin Forms |
| `DMax(expr, domain, criteria)` | Maximum matching value. | Shipped in Admin Forms |

Future work can broaden these beyond rendered Admin Forms and add more
diagnostics for expensive domain reads.

## Included Macro and Action Commands

### Record Actions

These map cleanly to existing form runtime behavior and should remain
declarative actions rather than host callbacks.

| Action | Purpose | Status |
| --- | --- | --- |
| `NewRecord` | Start a new record. | Shipped |
| `SaveRecord` | Save the current record. | Shipped |
| `DeleteRecord` | Delete the current record. | Shipped |
| `UndoRecord` | Revert unsaved edits. | Future action |
| `RefreshRecords` | Reload the current record/page. | Shipped |
| `Requery` | Reload the form record source. | Covered by `RefreshRecords`; broader record-source requery is future |
| `GoToRecord` | Navigate to a specific record. | Shipped |
| `FindRecord` | Search/navigate by criteria. | Future action |
| `NextRecord` | Navigate forward. | Shipped |
| `PreviousRecord` | Navigate backward. | Shipped |

### Form, Window, and Report Actions

| Action | Purpose | Status |
| --- | --- | --- |
| `OpenForm` | Open another saved form. | Shipped |
| `CloseForm` | Close current or named form. | Shipped |
| `OpenReport` | Open a saved report. | Future action |
| `CloseReport` | Close a report surface. | Future action |
| `PreviewReport` | Open report preview. | Future action |
| `PrintReport` | Print or export through report pipeline. | Future action |

### Filter and Sort Actions

| Action | Purpose | Status |
| --- | --- | --- |
| `ApplyFilter` | Apply a form or data-grid filter. | Shipped |
| `ClearFilter` | Clear current form or data-grid filter. | Shipped |
| `SetOrderBy` | Apply sort order. | Future action |
| `ClearOrderBy` | Clear sort order. | Future action |
| `SearchRecords` | Search over configured searchable fields. | Future action |

### UI and Control Actions

These are needed for Access-style command buttons and conditional workflows.

| Action | Purpose | Status |
| --- | --- | --- |
| `SetFieldValue` | Set a current record field value. | Shipped |
| `SetControlProperty` | Set visible/enabled/read-only/text/placeholder/value properties. | Shipped |
| `SetFocus` | Move focus to a control. | Future action |
| `SetControlEnabled` | Set enabled state. | Shipped |
| `SetControlVisibility` | Set visible state. | Shipped |
| `SetControlReadOnly` | Set read-only state. | Shipped |
| `ShowMessage` | Show a message through the current Forms surface. | Shipped |
| `InputBox` | Prompt for a value. | Future action |

### Flow Actions

| Action | Purpose | Status |
| --- | --- | --- |
| Per-step `Condition` | Conditionally run or skip a step. | Shipped |
| `Stop` | Stop the current action sequence successfully. | Shipped |
| `RunActionSequence` | Run a named reusable sequence. | Shipped |
| `If` / `Else` blocks | Conditional action branching. | Future action |
| `OnError` | Configure failure handling. | Future action |

### Data and Query Actions

These must be gated carefully because they can mutate data outside the current
form record.

| Action | Purpose | Status |
| --- | --- | --- |
| `OpenQuery` | Open a saved query result. | Future action |
| `RunQuery` | Execute a saved query. | Future trusted action |
| `RunProcedure` | Execute a saved procedure. | Shipped with host opt-in |
| `RunSql` | Execute SQL text. | Shipped with host opt-in |
| `ExportData` | Export records. | Future trusted action |
| `ImportData` | Import records. | Future trusted action |

### Code Bridge Actions

| Action | Purpose | Status |
| --- | --- | --- |
| `RunCommand` | Invoke host-registered trusted command callback. | Shipped |
| `RunCode` | Invoke database-owned C# code module function from a declarative action sequence. | Future action |

Database-owned C# form modules now compile and execute through a trusted
build/runtime model for form and control event handlers. `RunCode` remains a
future macro action that should reuse that model rather than embedding arbitrary
source text directly inside form JSON.

### Temp and Session Variables

| Action | Purpose | Status |
| --- | --- | --- |
| `SetTempVar` | Store a session-scoped value. | Future action |
| `RemoveTempVar` | Remove one session value. | Future action |
| `RemoveAllTempVars` | Clear session values. | Future action |

Temp variables should be scoped to the current user/session and be available to
form expressions, filters, and action sequences.

## Recommended Remaining Implementation Order

1. Add sort/search/find actions and any small aliases needed for Access naming
   compatibility.
2. Add temp/session variables and make them visible to expressions, filters,
   and action sequences.
3. Add richer macro flow such as `If` / `Else` blocks and `OnError`.
4. Add report/query/import/export actions behind explicit trusted boundaries.
5. Add `RunCode` after the code-module event-handler MVP is extended to macro
   action invocation.
6. Add file/app-launch style actions only as trusted operations.

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
