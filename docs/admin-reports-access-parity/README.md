# Admin Reports Access Parity Plan

This document captures the current Admin Reports review against Microsoft
Access-style report design, preview, print, and distribution expectations. It
focuses on gaps that affect whether CSharpDB reports can compete with Access for
database-backed operational reporting.

## Current Baseline

The current reports surface already includes:

- visual band-based report designer
- database-backed report metadata
- report sources from tables, views, and supported saved queries
- default report generation from source schema
- page settings for Letter/A4, portrait/landscape, and margins
- report header, page header, detail, page footer, report footer, group header,
  and group footer bands
- grouping and sorting definitions
- labels, bound text, calculated text, lines, and boxes
- calculated expressions for page number, print date, field references, numeric
  arithmetic, and `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` aggregates
- preview pagination with page headers and footers
- print support through the browser
- schema-drift warnings
- trusted command-backed preview lifecycle events for `OnOpen`,
  `BeforeRender`, and `AfterRender`

## Added Review Findings

### P1: Saved-query previews are unbounded before trimming

Saved-query reports execute `source.BaseSql` directly and only trim rows after
the full result has been materialized in memory. Large saved queries can make
preview slow, memory-heavy, or effectively unusable. Table and view reports use
the preview query builder with a row limit, but saved-query reports bypass that
path.

Primary code path:

- `src/CSharpDB.Admin.Reports/Services/DefaultReportPreviewService.cs`
- `src/CSharpDB.Admin.Reports/Services/ReportPreviewQueryBuilder.cs`

Expected fix:

- Apply the preview row cap before materializing saved-query results.
- Preserve report group/sort ordering by wrapping the saved query or using an
  equivalent safe query-builder path.
- Keep saved-query SQL validation parameterless unless parameter support is
  added in the same work.
- Add tests that prove large saved-query previews fetch only the capped row
  window.

### P2: Preview and print are capped, with no full output/export pipeline

The preview service intentionally caps output at `10,000` rows and `250` pages.
That is reasonable for an interactive preview, but Access-style reports also
need a full render/export path for print-ready output and distribution.

Primary code path:

- `src/CSharpDB.Admin.Reports/Services/DefaultReportPreviewService.cs`
- `src/CSharpDB.Admin.Reports/Pages/Preview.razor`

Expected fix:

- Separate preview rendering from full report rendering.
- Add export targets such as PDF, HTML, CSV, and spreadsheet-friendly output.
- Keep preview caps for the UI, but make full export explicit and cancellable.
- Show clear warnings when printing a capped preview rather than the full report.

## Access-Parity Roadmap

### Phase 1: Runtime Safety and Output Foundations

| Feature | Status | Notes |
| --- | --- | --- |
| Bounded saved-query previews | Planned | Apply preview row limits before materializing saved-query results. |
| Full report render pipeline | Planned | Separate capped preview from full print/export rendering. |
| Export support | Planned | Add PDF first, then HTML, CSV, and spreadsheet-friendly exports. |
| Print warnings | Planned | Make truncated preview printing explicit in the UI. |

### Phase 2: Record Sources, Parameters, and Filters

| Feature | Status | Notes |
| --- | --- | --- |
| Parameterized report sources | Planned | Support saved query/report parameters with prompt UI and typed values. |
| Runtime filters | Planned | Let users run a report with ad hoc filters without editing the design. |
| Saved report filter definitions | Planned | Store default filters with the report definition. |
| Source query builder integration | Planned | Let reports be based on query-designer definitions, not only raw tables/views/saved queries. |

### Phase 3: Grouping, Totals, and Pagination Semantics

| Feature | Status | Notes |
| --- | --- | --- |
| Grouping options | Planned | Add group intervals, header/footer toggles, keep together, repeat section, and force-new-page behavior. |
| Running totals | Planned | Add per-group and whole-report running sums. |
| Total placement helpers | Planned | Add guided totals in group headers, group footers, report header, and report footer. |
| Page header/footer options | Planned | Support Access-style choices such as suppressing page headers on report-header/report-footer pages. |
| Overflow behavior | Planned | Add text growth/shrink, clipping rules, and band overflow tests. |

### Phase 4: Design Productivity

| Feature | Status | Notes |
| --- | --- | --- |
| Layout View | Planned | Let users adjust a report while seeing real data. |
| Report Wizard | Planned | Guide source, fields, grouping, sorting, layout, and totals selection. |
| Label Wizard | Planned | Generate printable mailing/product labels from source fields. |
| Style themes | Planned | Add reusable report styles for fonts, spacing, borders, and colors. |

### Phase 5: Broader Report Controls and Formatting

| Feature | Status | Notes |
| --- | --- | --- |
| Control palette expansion | Planned | Add image/logo, rich text, barcode, chart, page break, subreport, and attachment/blob controls. |
| Conditional formatting | Planned | Add value/expression-based formatting rules for report controls. |
| Data bars and highlights | Planned | Add visual summaries for numeric ranges and thresholds. |
| Advanced formatting | Planned | Add borders, fill colors, font styles, text wrapping, alignment, culture-aware formats, and background images. |
| Subreports | Planned | Embed another report definition with parent/child linking. |

### Phase 6: Distribution and Operations

| Feature | Status | Notes |
| --- | --- | --- |
| Email/report delivery | Planned | Export and attach reports; trusted `AfterRender` commands provide an initial host callback but not a full delivery pipeline. |
| Scheduled reports | Research | Run recurring reports and store generated artifacts. |
| Report artifact history | Research | Store generated report snapshots for auditing and re-download. |
| Large-report cancellation | Planned | Add cancellation/progress for long render/export jobs. |

## Product Positioning

The current implementation is a useful printable preview engine with a banded
designer. To compete with Microsoft Access as a report builder, the next work
should move toward "reliable report production and distribution." The highest
leverage foundations are:

- bounded source execution
- separate preview and full-render pipelines
- parameter/filter model
- richer grouping and pagination model
- export/distribution model
- conditional formatting and expanded controls

Those foundations should come before broadening the designer surface too far.

## References

- [Introduction to reports in Access](https://support.microsoft.com/en-us/office/introduction-to-reports-in-access-e0869f59-7536-4d19-8e05-7158dcd3681c)
- [Create a simple report](https://support.microsoft.com/en-us/office/create-a-simple-report-408e92a8-11a4-418d-a378-7f1d99c25304)
- [Create a grouped or summary report](https://support.microsoft.com/en-au/office/create-a-grouped-or-summary-report-f23301a1-3e0a-4243-9002-4a23ac0fdbf3)
- [Summing in reports](https://support.microsoft.com/en-us/office/summing-in-reports-ad4e310d-64e9-4699-8d33-b8ae9639fbf4)
- [Highlight data with conditional formatting](https://support.microsoft.com/en-us/office/highlight-data-with-conditional-formatting-7f7c0bd4-7c37-421d-adad-a260125c8129)
- [Distribute a report](https://support.microsoft.com/en-us/office/distribute-a-report-561a9066-00ab-41ee-8f07-a0734810a778)
