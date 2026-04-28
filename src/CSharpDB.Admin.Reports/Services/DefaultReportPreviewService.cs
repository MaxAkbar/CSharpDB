using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Client;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Services;

public sealed class DefaultReportPreviewService(
    ICSharpDbClient dbClient,
    IReportSourceProvider sourceProvider,
    DbFunctionRegistry? functions = null) : IReportPreviewService
{
    internal const int MaxPreviewRows = 10000;
    internal const int MaxPreviewPages = 250;
    private const double PixelsPerInch = 96.0;

    public async Task<ReportPreviewResult> BuildPreviewAsync(ReportDefinition report, CancellationToken ct = default)
    {
        ReportSourceDefinition source = await sourceProvider.GetSourceDefinitionAsync(report.Source)
            ?? throw new InvalidOperationException($"Source '{report.Source.Name}' is no longer available.");

        IReadOnlyList<Dictionary<string, object?>> loadedRows = source.Kind switch
        {
            ReportSourceKind.SavedQuery => SortRowsInMemory(
                ReportSql.ReadRows(await dbClient.ExecuteSqlAsync(source.BaseSql, ct)),
                report),
            _ => ReportSql.ReadRows(await dbClient.ExecuteSqlAsync(ReportPreviewQueryBuilder.Build(report, source, MaxPreviewRows + 1), ct)),
        };
        bool rowTruncated = loadedRows.Count > MaxPreviewRows;
        List<Dictionary<string, object?>> rows = loadedRows.Take(MaxPreviewRows).ToList();

        IReadOnlyList<ReportPreviewPage> pages = Paginate(report, rows, functions ?? DbFunctionRegistry.Empty, out bool pageTruncated);
        bool hasSchemaDrift = !string.Equals(source.SourceSchemaSignature, report.SourceSchemaSignature, StringComparison.Ordinal);
        string? warning = BuildWarning(rowTruncated, pageTruncated, hasSchemaDrift);

        return new ReportPreviewResult(
            report,
            source,
            pages,
            rows.Count,
            rowTruncated || pageTruncated,
            hasSchemaDrift,
            warning,
            DateTime.UtcNow);
    }

    private static string? BuildWarning(bool rowTruncated, bool pageTruncated, bool hasSchemaDrift)
    {
        var warnings = new List<string>();
        if (rowTruncated)
            warnings.Add($"Preview limited to the first {MaxPreviewRows} rows.");
        if (pageTruncated)
            warnings.Add($"Preview limited to the first {MaxPreviewPages} pages.");
        if (hasSchemaDrift)
            warnings.Add("The source schema has changed since this report was last saved.");
        return warnings.Count == 0 ? null : string.Join(" ", warnings);
    }

    private static IReadOnlyList<Dictionary<string, object?>> SortRowsInMemory(IReadOnlyList<Dictionary<string, object?>> rows, ReportDefinition report)
    {
        var sortSpecs = new List<(string FieldName, bool Descending)>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (ReportGroupDefinition group in report.Groups)
        {
            if (seen.Add(group.FieldName))
                sortSpecs.Add((group.FieldName, group.Descending));
        }

        foreach (ReportSortDefinition sort in report.Sorts)
        {
            if (seen.Add(sort.FieldName))
                sortSpecs.Add((sort.FieldName, sort.Descending));
        }

        if (sortSpecs.Count == 0)
            return rows;

        List<Dictionary<string, object?>> ordered = rows.ToList();
        ordered.Sort((left, right) => CompareRows(left, right, sortSpecs));
        return ordered;
    }

    private static int CompareRows(IReadOnlyDictionary<string, object?> left, IReadOnlyDictionary<string, object?> right, IReadOnlyList<(string FieldName, bool Descending)> sortSpecs)
    {
        foreach ((string fieldName, bool descending) in sortSpecs)
        {
            int comparison = CompareValues(LookupFieldValue(left, fieldName), LookupFieldValue(right, fieldName));
            if (comparison == 0)
                continue;

            return descending ? -comparison : comparison;
        }

        return 0;
    }

    private static int CompareValues(object? left, object? right)
    {
        object? normalizedLeft = ReportSql.NormalizeValue(left);
        object? normalizedRight = ReportSql.NormalizeValue(right);

        if (normalizedLeft is null && normalizedRight is null)
            return 0;
        if (normalizedLeft is null)
            return -1;
        if (normalizedRight is null)
            return 1;

        if (normalizedLeft is string leftText && normalizedRight is string rightText)
            return StringComparer.OrdinalIgnoreCase.Compare(leftText, rightText);

        if (normalizedLeft is IComparable comparableLeft && normalizedRight.GetType() == normalizedLeft.GetType())
            return comparableLeft.CompareTo(normalizedRight);

        if (ReportSql.TryConvertToDouble(normalizedLeft, out double leftNumber)
            && ReportSql.TryConvertToDouble(normalizedRight, out double rightNumber))
        {
            return leftNumber.CompareTo(rightNumber);
        }

        return StringComparer.OrdinalIgnoreCase.Compare(
            Convert.ToString(normalizedLeft, System.Globalization.CultureInfo.InvariantCulture),
            Convert.ToString(normalizedRight, System.Globalization.CultureInfo.InvariantCulture));
    }

    private static IReadOnlyList<ReportPreviewPage> Paginate(
        ReportDefinition report,
        List<Dictionary<string, object?>> rows,
        DbFunctionRegistry functions,
        out bool pageTruncated)
    {
        pageTruncated = false;
        var pages = new List<ReportPreviewPage>();
        bool truncated = false;

        ReportBandDefinition? reportHeader = FindBand(report, ReportBandKind.ReportHeader, null);
        ReportBandDefinition? pageHeader = FindBand(report, ReportBandKind.PageHeader, null);
        ReportBandDefinition? detailBand = FindBand(report, ReportBandKind.Detail, null);
        ReportBandDefinition? pageFooter = FindBand(report, ReportBandKind.PageFooter, null);
        ReportBandDefinition? reportFooter = FindBand(report, ReportBandKind.ReportFooter, null);

        double pageHeightPx = GetPageHeightPx(report.PageSettings);
        double topMarginPx = report.PageSettings.MarginTopInches * PixelsPerInch;
        double bottomMarginPx = report.PageSettings.MarginBottomInches * PixelsPerInch;
        double availableHeight = Math.Max(100, pageHeightPx - topMarginPx - bottomMarginPx);
        double headerHeight = pageHeader?.Height ?? 0;
        double footerHeight = pageFooter?.Height ?? 0;
        double bodyHeight = Math.Max(100, availableHeight - headerHeight - footerHeight);

        List<ReportRenderedBand>? currentBands = null;
        double remainingBodyHeight = 0;
        DateTime generatedUtc = DateTime.UtcNow;

        void StartPage()
        {
            if (currentBands is not null)
                FinalizePage();

            if (pages.Count >= MaxPreviewPages)
            {
                truncated = true;
                return;
            }

            currentBands = [];
            remainingBodyHeight = bodyHeight;

            if (pageHeader is not null)
                currentBands.Add(RenderBand(pageHeader, row: null, rows: [], pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions));
        }

        void FinalizePage()
        {
            if (currentBands is null)
                return;

            if (pageFooter is not null)
                currentBands.Add(RenderBand(pageFooter, row: null, rows: [], pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions));

            pages.Add(new ReportPreviewPage(pages.Count + 1, currentBands.ToArray()));
            currentBands = null;
        }

        bool TryAddBand(ReportRenderedBand band)
        {
            if (currentBands is null)
                StartPage();

            if (truncated || currentBands is null)
                return false;

            int nonBodyBandCount = pageHeader is null ? 0 : 1;
            if (band.Height > remainingBodyHeight && currentBands.Count > nonBodyBandCount)
            {
                StartPage();
                if (truncated || currentBands is null)
                    return false;
            }

            currentBands.Add(band);
            remainingBodyHeight -= band.Height;
            return true;
        }

        StartPage();
        if (truncated || currentBands is null)
        {
            pageTruncated = truncated;
            return pages;
        }

        if (reportHeader is not null && !TryAddBand(RenderBand(reportHeader, row: rows.FirstOrDefault(), rows: rows, pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions)))
        {
            pageTruncated = truncated;
            return pages;
        }

        var previousGroupValues = new object?[report.Groups.Count];
        var groupStartIndices = new int[report.Groups.Count];
        bool havePreviousRow = false;

        for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            Dictionary<string, object?> row = rows[rowIndex];
            int changedGroupIndex = FindChangedGroupIndex(report.Groups, previousGroupValues, row, havePreviousRow);

            if (havePreviousRow && changedGroupIndex >= 0)
            {
                for (int groupIndex = report.Groups.Count - 1; groupIndex >= changedGroupIndex; groupIndex--)
                {
                    ReportGroupDefinition group = report.Groups[groupIndex];
                    if (!group.ShowFooter)
                        continue;

                    ReportBandDefinition? footerBand = FindBand(report, ReportBandKind.GroupFooter, group.GroupId);
                    if (footerBand is null)
                        continue;

                    IReadOnlyList<Dictionary<string, object?>> groupRows = rows.Skip(groupStartIndices[groupIndex]).Take(rowIndex - groupStartIndices[groupIndex]).ToArray();
                    if (!TryAddBand(RenderBand(footerBand, row: rows[rowIndex - 1], rows: groupRows, pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions)))
                    {
                        pageTruncated = truncated;
                        return pages;
                    }
                }
            }

            if (changedGroupIndex >= 0)
            {
                for (int groupIndex = changedGroupIndex; groupIndex < report.Groups.Count; groupIndex++)
                {
                    ReportGroupDefinition group = report.Groups[groupIndex];
                    previousGroupValues[groupIndex] = LookupFieldValue(row, group.FieldName);
                    groupStartIndices[groupIndex] = rowIndex;

                    if (!group.ShowHeader)
                        continue;

                    ReportBandDefinition? headerBand = FindBand(report, ReportBandKind.GroupHeader, group.GroupId);
                    if (headerBand is null)
                        continue;

                    if (!TryAddBand(RenderBand(headerBand, row: row, rows: [row], pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions)))
                    {
                        pageTruncated = truncated;
                        return pages;
                    }
                }
            }

            havePreviousRow = true;

            if (detailBand is not null && !TryAddBand(RenderBand(detailBand, row: row, rows: [row], pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions)))
            {
                pageTruncated = truncated;
                return pages;
            }
        }

        if (havePreviousRow)
        {
            for (int groupIndex = report.Groups.Count - 1; groupIndex >= 0; groupIndex--)
            {
                ReportGroupDefinition group = report.Groups[groupIndex];
                if (!group.ShowFooter)
                    continue;

                ReportBandDefinition? footerBand = FindBand(report, ReportBandKind.GroupFooter, group.GroupId);
                if (footerBand is null)
                    continue;

                IReadOnlyList<Dictionary<string, object?>> groupRows = rows.Skip(groupStartIndices[groupIndex]).ToArray();
                if (!TryAddBand(RenderBand(footerBand, row: rows[^1], rows: groupRows, pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions)))
                {
                    pageTruncated = truncated;
                    return pages;
                }
            }
        }

        if (reportFooter is not null)
            TryAddBand(RenderBand(reportFooter, row: rows.LastOrDefault(), rows: rows, pageNumber: pages.Count + 1, generatedUtc: generatedUtc, functions: functions));

        FinalizePage();
        pageTruncated = truncated;
        return pages;
    }

    private static int FindChangedGroupIndex(IReadOnlyList<ReportGroupDefinition> groups, object?[] previousGroupValues, IReadOnlyDictionary<string, object?> row, bool havePreviousRow)
    {
        if (groups.Count == 0)
            return -1;

        if (!havePreviousRow)
            return 0;

        for (int i = 0; i < groups.Count; i++)
        {
            object? currentValue = LookupFieldValue(row, groups[i].FieldName);
            if (!Equals(ReportSql.NormalizeValue(previousGroupValues[i]), ReportSql.NormalizeValue(currentValue)))
                return i;
        }

        return -1;
    }

    private static ReportBandDefinition? FindBand(ReportDefinition report, ReportBandKind kind, string? groupId)
        => report.Bands.FirstOrDefault(band => band.BandKind == kind && string.Equals(band.GroupId, groupId, StringComparison.Ordinal));

    private static ReportRenderedBand RenderBand(
        ReportBandDefinition band,
        IReadOnlyDictionary<string, object?>? row,
        IReadOnlyList<Dictionary<string, object?>> rows,
        int pageNumber,
        DateTime generatedUtc,
        DbFunctionRegistry functions)
    {
        ReportRenderedControl[] renderedControls = band.Controls
            .Select(control => RenderControl(control, row, rows, pageNumber, generatedUtc, functions))
            .ToArray();
        return new ReportRenderedBand(band.BandId, band.BandKind, band.GroupId, band.Height, renderedControls);
    }

    private static ReportRenderedControl RenderControl(
        ReportControlDefinition control,
        IReadOnlyDictionary<string, object?>? row,
        IReadOnlyList<Dictionary<string, object?>> rows,
        int pageNumber,
        DateTime generatedUtc,
        DbFunctionRegistry functions)
    {
        string? text = control.ControlType switch
        {
            ReportControlType.Label => LookupProp(control.Props, "text"),
            ReportControlType.BoundText => ReportSql.FormatDisplayValue(LookupFieldValue(row, control.BoundFieldName), control.FormatString),
            ReportControlType.CalculatedText => RenderCalculatedText(control, row, rows, pageNumber, generatedUtc, functions),
            _ => null,
        };

        return new ReportRenderedControl(control.ControlId, control.ControlType, control.Rect, text, control.Props);
    }

    private static string RenderCalculatedText(
        ReportControlDefinition control,
        IReadOnlyDictionary<string, object?>? row,
        IReadOnlyList<Dictionary<string, object?>> rows,
        int pageNumber,
        DateTime generatedUtc,
        DbFunctionRegistry functions)
    {
        string expression = control.Expression?.Trim() ?? string.Empty;
        string? prefix = LookupProp(control.Props, "prefix");

        string value = expression switch
        {
            "=PageNumber" => pageNumber.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "=PrintDate" => ReportSql.FormatDisplayValue(generatedUtc, control.FormatString),
            _ when ReportFormulaEvaluator.TryParseAggregate(expression, out string functionName, out string fieldName)
                => ReportSql.FormatDisplayValue(ReportFormulaEvaluator.EvaluateAggregate(functionName, rows.Select(item => LookupFieldValue(item, fieldName))), control.FormatString),
            _ when ReportFormulaEvaluator.TryEvaluateScalar(expression, field => LookupFieldValue(row, field), functions, out object? scalarValue)
                => ReportSql.FormatDisplayValue(scalarValue, control.FormatString),
            _ when row is not null && ReportFormulaEvaluator.TryReadFieldReference(expression.TrimStart('='), out string boundFieldName)
                => ReportSql.FormatDisplayValue(LookupFieldValue(row, boundFieldName), control.FormatString),
            _ when row is not null
                => ReportSql.FormatDisplayValue(
                    ReportFormulaEvaluator.EvaluateNumeric(expression, field =>
                    {
                        object? fieldValue = LookupFieldValue(row, field);
                        return ReportSql.TryConvertToDouble(fieldValue, out double numeric) ? numeric : null;
                    }, functions),
                    control.FormatString),
            _ => string.Empty,
        };

        return string.IsNullOrWhiteSpace(prefix) ? value : $"{prefix}{value}";
    }

    private static object? LookupFieldValue(IReadOnlyDictionary<string, object?>? row, string? fieldName)
    {
        if (row is null || string.IsNullOrWhiteSpace(fieldName))
            return null;

        if (row.TryGetValue(fieldName, out object? value))
            return value;

        string? actualKey = row.Keys.FirstOrDefault(key => string.Equals(key, fieldName, StringComparison.OrdinalIgnoreCase));
        return actualKey is not null && row.TryGetValue(actualKey, out value) ? value : null;
    }

    private static string? LookupProp(PropertyBag props, string key)
        => props.Values.TryGetValue(key, out object? value) ? value?.ToString() : null;

    private static double GetPageHeightPx(ReportPageSettings settings)
    {
        (double widthInches, double heightInches) = settings.PaperSize.ToUpperInvariant() switch
        {
            "A4" => (8.27, 11.69),
            _ => (8.5, 11.0),
        };

        if (string.Equals(settings.Orientation, "Landscape", StringComparison.OrdinalIgnoreCase))
            (widthInches, heightInches) = (heightInches, widthInches);

        return heightInches * PixelsPerInch;
    }
}
