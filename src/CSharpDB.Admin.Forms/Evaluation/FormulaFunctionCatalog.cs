namespace CSharpDB.Admin.Forms.Evaluation;

public sealed record FormulaFunctionDescriptor(
    string Name,
    string Category,
    string Signature,
    string Description,
    string Example,
    string InsertText);

public static class FormulaFunctionCatalog
{
    public static IReadOnlyList<FormulaFunctionDescriptor> ExpressionFunctions { get; } =
    [
        new("Nz", "Null and Conditional", "Nz(value, fallback)", "Returns fallback when value is null or empty.", "=Nz(Quantity, 0)", "Nz(value, fallback)"),
        new("IsNull", "Null and Conditional", "IsNull(value)", "Returns true when value is null.", "=IsNull(ClosedDate)", "IsNull(value)"),
        new("IsEmpty", "Null and Conditional", "IsEmpty(value)", "Returns true when value is null or empty text.", "=IsEmpty(Notes)", "IsEmpty(value)"),
        new("IIf", "Null and Conditional", "IIf(condition, trueValue, falseValue)", "Returns one of two values based on a condition.", "=IIf(Status = 'Closed', 'Done', 'Open')", "IIf(condition, trueValue, falseValue)"),
        new("Switch", "Null and Conditional", "Switch(condition1, value1, ...)", "Returns the first value whose condition is true.", "=Switch(Priority = 1, 'High', Priority = 2, 'Normal')", "Switch(condition1, value1, condition2, value2)"),
        new("Choose", "Null and Conditional", "Choose(index, value1, value2, ...)", "Returns the value at a 1-based position.", "=Choose(StatusCode, 'New', 'Open', 'Closed')", "Choose(index, value1, value2)"),

        new("Len", "Text", "Len(value)", "Returns text length.", "=Len(CustomerName)", "Len(value)"),
        new("Left", "Text", "Left(value, count)", "Returns characters from the left side.", "=Left(CustomerCode, 3)", "Left(value, count)"),
        new("Right", "Text", "Right(value, count)", "Returns characters from the right side.", "=Right(OrderNumber, 4)", "Right(value, count)"),
        new("Mid", "Text", "Mid(value, start, count)", "Returns characters from the middle of text.", "=Mid(ProductCode, 2, 3)", "Mid(value, start, count)"),
        new("Trim", "Text", "Trim(value)", "Trims leading and trailing spaces.", "=Trim(CustomerName)", "Trim(value)"),
        new("LTrim", "Text", "LTrim(value)", "Trims leading spaces.", "=LTrim(Code)", "LTrim(value)"),
        new("RTrim", "Text", "RTrim(value)", "Trims trailing spaces.", "=RTrim(Code)", "RTrim(value)"),
        new("UCase", "Text", "UCase(value)", "Converts text to uppercase.", "=UCase(CustomerName)", "UCase(value)"),
        new("LCase", "Text", "LCase(value)", "Converts text to lowercase.", "=LCase(Email)", "LCase(value)"),
        new("InStr", "Text", "InStr(value, search)", "Returns the 1-based position of search text, or 0.", "=InStr(Email, '@')", "InStr(value, search)"),
        new("Replace", "Text", "Replace(value, search, replacement)", "Replaces matching text.", "=Replace(Phone, '-', '')", "Replace(value, search, replacement)"),
        new("StrComp", "Text", "StrComp(left, right, comparison)", "Compares two strings and returns -1, 0, or 1.", "=StrComp(Code, 'A1', 'text')", "StrComp(left, right, 'text')"),
        new("Val", "Text", "Val(value)", "Parses leading numeric text.", "=Val(QuantityText)", "Val(value)"),

        new("Date", "Date and Time", "Date()", "Returns today's date.", "=Date()", "Date()"),
        new("Time", "Date and Time", "Time()", "Returns the current time.", "=Time()", "Time()"),
        new("Now", "Date and Time", "Now()", "Returns the current date and time.", "=Now()", "Now()"),
        new("Year", "Date and Time", "Year(value)", "Returns the year number.", "=Year(OrderDate)", "Year(value)"),
        new("Month", "Date and Time", "Month(value)", "Returns the month number.", "=Month(OrderDate)", "Month(value)"),
        new("Day", "Date and Time", "Day(value)", "Returns the day of month.", "=Day(OrderDate)", "Day(value)"),
        new("Hour", "Date and Time", "Hour(value)", "Returns the hour.", "=Hour(UpdatedAt)", "Hour(value)"),
        new("Minute", "Date and Time", "Minute(value)", "Returns the minute.", "=Minute(UpdatedAt)", "Minute(value)"),
        new("Second", "Date and Time", "Second(value)", "Returns the second.", "=Second(UpdatedAt)", "Second(value)"),
        new("DateAdd", "Date and Time", "DateAdd(interval, amount, value)", "Adds a date/time interval.", "=DateAdd('d', 7, OrderDate)", "DateAdd('d', amount, value)"),
        new("DateDiff", "Date and Time", "DateDiff(interval, start, end)", "Returns the difference between dates.", "=DateDiff('d', OrderDate, Date())", "DateDiff('d', start, end)"),
        new("DatePart", "Date and Time", "DatePart(interval, value)", "Returns a date/time part.", "=DatePart('q', OrderDate)", "DatePart('q', value)"),
        new("DateSerial", "Date and Time", "DateSerial(year, month, day)", "Builds a date.", "=DateSerial(Year(Date()), 1, 1)", "DateSerial(year, month, day)"),
        new("TimeSerial", "Date and Time", "TimeSerial(hour, minute, second)", "Builds a time value.", "=TimeSerial(17, 0, 0)", "TimeSerial(hour, minute, second)"),
        new("Weekday", "Date and Time", "Weekday(value)", "Returns day of week as 1 through 7.", "=Weekday(OrderDate)", "Weekday(value)"),
        new("MonthName", "Date and Time", "MonthName(month)", "Returns the month name.", "=MonthName(Month(OrderDate))", "MonthName(month)"),

        new("Abs", "Number and Conversion", "Abs(value)", "Returns absolute value.", "=Abs(Balance)", "Abs(value)"),
        new("Round", "Number and Conversion", "Round(value, digits)", "Rounds a number.", "=Round(Amount, 2)", "Round(value, digits)"),
        new("Int", "Number and Conversion", "Int(value)", "Rounds down to an integer.", "=Int(Amount)", "Int(value)"),
        new("Fix", "Number and Conversion", "Fix(value)", "Truncates toward zero.", "=Fix(Amount)", "Fix(value)"),
        new("Sgn", "Number and Conversion", "Sgn(value)", "Returns -1, 0, or 1.", "=Sgn(Balance)", "Sgn(value)"),
        new("CStr", "Number and Conversion", "CStr(value)", "Converts a value to text.", "=CStr(OrderId)", "CStr(value)"),
        new("CInt", "Number and Conversion", "CInt(value)", "Converts a value to an integer.", "=CInt(QuantityText)", "CInt(value)"),
        new("CLng", "Number and Conversion", "CLng(value)", "Converts a value to a long integer.", "=CLng(IdText)", "CLng(value)"),
        new("CDbl", "Number and Conversion", "CDbl(value)", "Converts a value to a double.", "=CDbl(AmountText)", "CDbl(value)"),
        new("CBool", "Number and Conversion", "CBool(value)", "Converts a value to boolean.", "=CBool(IsActive)", "CBool(value)"),
        new("CDate", "Number and Conversion", "CDate(value)", "Converts a value to date/time.", "=CDate(DateText)", "CDate(value)"),
        new("Format", "Number and Conversion", "Format(value, format)", "Formats a number, date, time, or text.", "=Format(Amount, '0.00')", "Format(value, format)"),

        new("DLookup", "Domain", "DLookup(expr, domain, criteria)", "Returns one value from another table/query.", "=DLookup('Name', 'Customers', 'CustomerId = 1')", "DLookup('FieldName', 'TableName', 'Field = value')"),
        new("DCount", "Domain", "DCount(expr, domain, criteria)", "Counts matching rows in another table/query.", "=DCount('*', 'Orders', 'Status = ''Open''')", "DCount('*', 'TableName', 'Field = value')"),
        new("DSum", "Domain", "DSum(expr, domain, criteria)", "Sums matching rows in another table/query.", "=DSum('Amount', 'OrderLines', 'OrderId = 1')", "DSum('FieldName', 'TableName', 'Field = value')"),
        new("DAvg", "Domain", "DAvg(expr, domain, criteria)", "Averages matching rows in another table/query.", "=DAvg('Amount', 'OrderLines', 'OrderId = 1')", "DAvg('FieldName', 'TableName', 'Field = value')"),
        new("DMin", "Domain", "DMin(expr, domain, criteria)", "Returns the minimum matching value.", "=DMin('Amount', 'OrderLines', 'OrderId = 1')", "DMin('FieldName', 'TableName', 'Field = value')"),
        new("DMax", "Domain", "DMax(expr, domain, criteria)", "Returns the maximum matching value.", "=DMax('Amount', 'OrderLines', 'OrderId = 1')", "DMax('FieldName', 'TableName', 'Field = value')"),
    ];

    public static IReadOnlyList<FormulaFunctionDescriptor> AggregateFunctions { get; } =
    [
        new("SUM", "Child Aggregates", "SUM(Table.Field)", "Sums child-row values for the current parent record.", "=SUM(OrderLines.Amount)", "SUM(Table.Field)"),
        new("COUNT", "Child Aggregates", "COUNT(Table.Field)", "Counts child-row values for the current parent record.", "=COUNT(OrderLines.Id)", "COUNT(Table.Field)"),
        new("AVG", "Child Aggregates", "AVG(Table.Field)", "Averages child-row values for the current parent record.", "=AVG(OrderLines.Amount)", "AVG(Table.Field)"),
        new("MIN", "Child Aggregates", "MIN(Table.Field)", "Returns the minimum child-row value.", "=MIN(OrderLines.Amount)", "MIN(Table.Field)"),
        new("MAX", "Child Aggregates", "MAX(Table.Field)", "Returns the maximum child-row value.", "=MAX(OrderLines.Amount)", "MAX(Table.Field)"),
    ];

    public static IReadOnlyList<FormulaFunctionDescriptor> AllFunctions { get; } =
        ExpressionFunctions.Concat(AggregateFunctions).ToArray();

    public static IReadOnlyList<string> Categories { get; } =
        AllFunctions
            .Select(static function => function.Category)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

    public static IReadOnlyCollection<string> BuiltInFunctionNames { get; } =
        ExpressionFunctions.Select(static function => function.Name).ToArray();
}
