# Admin Forms Access Parity Plan

This document captures the current Admin Forms review against Microsoft Access-style
form design and data-entry expectations. It focuses on gaps that affect whether
CSharpDB forms can compete with Access for database-backed line-of-business apps.

## Current Baseline

The current forms surface already includes:

- drag/drop absolute-layout designer
- generated forms from table and view schema
- database-backed form metadata
- runtime data entry with create, update, delete, paging, and navigation
- record search and go-to-primary-key navigation
- labels, text, textarea, number, date, checkbox, radio, select, lookup,
  computed, data grid, and child-tabs controls
- lookup lists loaded from tables
- computed fields with simple formulas and child-table aggregates
- one-to-many child grids and nested child tabs
- print support
- schema-change warnings
- designer undo/redo, copy/paste, duplicate, layers, alignment, tab order, and
  mobile/tablet/desktop breakpoint editing

## Added Review Findings

### P1: Runtime ignores responsive form layouts

The designer stores mobile and tablet overrides in `RendererHints`, but the
runtime renderer always uses `ControlDefinition.Rect`. Any breakpoint layout work
saved in the designer will not affect actual data-entry rendering.

Primary code path:

- `src/CSharpDB.Admin.Forms/Components/Designer/FormRenderer.razor`
- `src/CSharpDB.Admin.Forms/Components/Designer/DesignerState.cs`

Expected fix:

- Add a runtime breakpoint/layout resolver shared with the designer.
- Render controls from the effective breakpoint rectangle, not only the desktop
  rectangle.
- Honor breakpoint visibility at runtime.
- Add renderer tests for desktop, tablet, and mobile overrides.

### P1: Inferred validation is not enforced

`InferRules` creates required, range, regex, and one-of rules, but `Evaluate`
currently checks only `maxLength` and manually added `required` rules. Default
generated forms can save invalid required, range, regex, or enum data unless the
database rejects it later.

Primary code path:

- `src/CSharpDB.Admin.Forms/Services/DefaultValidationInferenceService.cs`
- `src/CSharpDB.Admin.Forms/Services/DefaultFormGenerator.cs`

Expected fix:

- Evaluate inferred `required`, `maxLength`, `range`, `regex`, and `oneOf`
  rules for generated controls.
- Keep validation override behavior intact.
- Normalize numeric and choice values before comparing.
- Add tests for generated forms and override combinations.

## Access-Parity Roadmap

### Phase 1: Correctness Before Expansion

| Feature | Status | Notes |
| --- | --- | --- |
| Runtime breakpoint rendering | Planned | Make mobile/tablet designer work visible in data entry. |
| Complete inferred validation | Planned | Enforce generated required/range/regex/choice rules before save. |
| Form runtime regression tests | Planned | Cover renderer layout, validation, lookup, computed, and child grid behavior. |

### Phase 2: Record Source, Filtering, and Sorting

| Feature | Status | Notes |
| --- | --- | --- |
| First-class record source model | Planned | Move beyond a single `TableName`; support table, view, and saved SQL/query sources with editability metadata. |
| Default filter and default sort | Planned | Store form-level defaults and apply them in the runtime record service. |
| User sorting | Planned | Let operators sort by visible/searchable fields at runtime. |
| Advanced filtering | Planned | Add filter-by-field, filter-by-selection, multi-condition filters, saved filters, and clear-filter flows. |

### Phase 3: Access-Style Form Experiences

| Feature | Status | Notes |
| --- | --- | --- |
| Layout View | Planned | Let designers adjust a form while real data is visible. |
| Multiple form modes | Planned | Add Single Form, Multiple Items, Datasheet, and Split Form equivalents. |
| Form sections | Planned | Add header, detail, footer, and optional print sections. |
| Embedded subforms | Planned | Support arbitrary embedded form definitions, not only child grids/tabs. |

### Phase 4: Actions, Events, and App Behavior

| Feature | Status | Notes |
| --- | --- | --- |
| Command button control | Planned | Add buttons that can run form actions. |
| Action model | Planned | Support actions such as open form, save, delete, navigate, apply filter, clear filter, run SQL/procedure, and show message. |
| Event hooks | Planned | Add form/control events such as on load, before save, after save, before field change, after field change, and button click. |
| Conditional UI rules | Planned | Add visible/enabled/read-only expressions for controls. |

### Phase 5: Broader Control and Property Coverage

| Feature | Status | Notes |
| --- | --- | --- |
| Control palette expansion | Planned | Add list box, option group, toggle button, image, attachment/blob, chart, navigation, line, rectangle, page break, and subreport-like controls. |
| Formatting properties | Planned | Add font, color, border, alignment, numeric/date format, input mask, default value, and required indicators. |
| Lookup improvements | Planned | Add searchable combo behavior, display/value column configuration, row limits, and dependent lookups. |
| Child grid improvements | Planned | Add typed editors, validation, sort/filter, column sizing, and batch edit behavior. |

## Product Positioning

The current implementation is a strong database-backed generated-form system.
To credibly compete with Microsoft Access as an app builder, the next work should
move from "render fields over records" toward "design complete data-entry
workflows." The highest leverage model changes are:

- runtime layout resolver
- full validation engine
- richer record-source/filter/sort model
- action/event model
- form-mode model

Those foundations should be added before expanding the control palette too far.
