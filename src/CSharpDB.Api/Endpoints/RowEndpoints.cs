using CSharpDB.Api.Dtos;
using CSharpDB.Api.Helpers;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Endpoints;

public static class RowEndpoints
{
    public static RouteGroupBuilder MapRowEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/table-operations/rows", BrowseRows);
        group.MapGet("/table-operations/row", GetRowByPk);
        group.MapPost("/table-operations/rows", InsertRow);
        group.MapPut("/table-operations/row", UpdateRow);
        group.MapDelete("/table-operations/row", DeleteRow);

        // Backward-compatible path routes. Query-based routes are canonical
        // because route segments cannot faithfully carry every quoted SQL
        // identifier or primary-key string.
        group.MapGet("/tables/{name}/rows", BrowseRows);
        group.MapGet("/tables/{name}/rows/{pkValue}", GetRowByPk);
        group.MapPost("/tables/{name}/rows", InsertRow);
        group.MapPut("/tables/{name}/rows/{pkValue}", UpdateRow);
        group.MapDelete("/tables/{name}/rows/{pkValue}", DeleteRow);

        return group;
    }

    private static async Task<IResult> BrowseRows(
        string name, ICSharpDbClient db, int page = 1, int pageSize = 50)
    {
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 50;
        if (pageSize > 1000) pageSize = 1000;

        var result = await db.BrowseTableAsync(name, page, pageSize);

        var columnNames = result.Schema.Columns.Select(c => c.Name).ToArray();
        var rows = JsonHelper.RowsToNamedDictionaries(columnNames, result.Rows);

        return Results.Ok(new BrowseResponse(
            columnNames,
            rows,
            result.TotalRows,
            result.Page,
            result.PageSize,
            result.TotalPages,
            result.Schema.Columns.Select(column =>
                column.IsRowVersion
                    ? "ROWVERSION"
                    : column.EffectiveType.ToSql()).ToArray()));
    }

    private static async Task<IResult> GetRowByPk(
        string name,
        string pkValue,
        ICSharpDbClient db,
        string pkColumn = "id",
        string? pkEncoding = null,
        string? pkType = null)
    {
        object coerced = await CoercePkValueAsync(
            name,
            pkColumn,
            pkValue,
            pkEncoding,
            pkType,
            db);
        var row = await db.GetRowByPkAsync(name, pkColumn, coerced);
        return row is null
            ? Results.NotFound(new { error = "The requested row was not found." })
            : Results.Ok(JsonHelper.EncodeDictionary(row));
    }

    private static async Task<IResult> InsertRow(string name, InsertRowRequest req, ICSharpDbClient db)
    {
        var values = JsonHelper.CoerceDictionary(req.Values);
        var affected = await db.InsertRowAsync(name, values);
        return Results.Created(
            $"/api/table-operations/rows?name={Uri.EscapeDataString(name)}",
            new MutationResponse(affected));
    }

    private static async Task<IResult> UpdateRow(
        string name,
        string pkValue,
        UpdateRowRequest req,
        ICSharpDbClient db,
        string pkColumn = "id",
        string? pkEncoding = null,
        string? pkType = null)
    {
        object coerced = await CoercePkValueAsync(
            name,
            pkColumn,
            pkValue,
            pkEncoding,
            pkType,
            db);
        var values = JsonHelper.CoerceDictionary(req.Values);
        var affected = await db.UpdateRowAsync(name, pkColumn, coerced, values);
        return Results.Ok(new MutationResponse(affected));
    }

    private static async Task<IResult> DeleteRow(
        string name,
        string pkValue,
        ICSharpDbClient db,
        string pkColumn = "id",
        string? pkEncoding = null,
        string? pkType = null)
    {
        object coerced = await CoercePkValueAsync(
            name,
            pkColumn,
            pkValue,
            pkEncoding,
            pkType,
            db);
        var affected = await db.DeleteRowAsync(name, pkColumn, coerced);
        return affected == 0
            ? Results.NotFound(new { error = "The requested row was not found." })
            : Results.Ok(new MutationResponse(affected));
    }

    /// <summary>
    /// PK values from routes are strings. Binary keys carry a separate
    /// encoding marker so ordinary string keys remain unambiguous.
    /// </summary>
    private static async Task<object> CoercePkValueAsync(
        string tableName,
        string pkColumn,
        string raw,
        string? encoding,
        string? transportType,
        ICSharpDbClient db)
    {
        string? normalizedTransportType = string.IsNullOrWhiteSpace(transportType)
            ? null
            : transportType.Trim().ToLowerInvariant();
        ValidateKeyTransportType(normalizedTransportType);
        if (!string.IsNullOrWhiteSpace(encoding))
        {
            if (!string.Equals(encoding, "base64", StringComparison.OrdinalIgnoreCase))
            {
                throw new BadHttpRequestException(
                    $"Unsupported primary-key encoding '{encoding}'.",
                    StatusCodes.Status400BadRequest);
            }

            try
            {
                return Convert.FromBase64String(raw);
            }
            catch (FormatException)
            {
                throw new BadHttpRequestException(
                    "The binary primary key contains invalid base64 data.",
                    StatusCodes.Status400BadRequest);
            }
        }

        TableSchema? schema = await db.GetTableSchemaAsync(tableName);
        SqlTypeKind? logicalType = schema?.Columns
            .FirstOrDefault(column =>
                string.Equals(column.Name, pkColumn, StringComparison.OrdinalIgnoreCase))
            ?.EffectiveType.Kind;

        object? numericKey = logicalType switch
        {
            SqlTypeKind.Boolean => ParseBooleanKey(raw),
            SqlTypeKind.TinyInt or
            SqlTypeKind.SmallInt or
            SqlTypeKind.Integer or
            SqlTypeKind.BigInt => ParseIntegerKey(raw),
            SqlTypeKind.Real or SqlTypeKind.Double => ParseRealKey(raw),
            SqlTypeKind.Decimal => ParseDecimalKey(raw),
            _ => null,
        };

        if (numericKey is not null)
            return numericKey;

        // The schema is authoritative for text-backed logical types. This
        // deliberately preserves values such as "123.4500" in a TEXT key,
        // even if a caller supplied a numeric-looking transport marker.
        if (logicalType is not null)
            return raw;

        // Retain compatibility with servers that cannot expose the schema.
        return CoerceKeyFromTransportType(raw, normalizedTransportType);
    }

    private static object CoerceKeyFromTransportType(string raw, string? transportType) =>
        transportType switch
        {
            "boolean" => ParseBooleanKey(raw),
            "integer" => ParseIntegerKey(raw),
            "real" => ParseRealKey(raw),
            "decimal" => ParseDecimalKey(raw),
            _ => raw,
        };

    private static long ParseBooleanKey(string raw)
    {
        if (bool.TryParse(raw, out bool boolean))
            return boolean ? 1L : 0L;

        if (long.TryParse(
                raw,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out long integer) &&
            integer is 0 or 1)
        {
            return integer;
        }

        throw InvalidKey("a boolean", raw);
    }

    private static long ParseIntegerKey(string raw)
    {
        if (long.TryParse(
                raw,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out long integer))
        {
            return integer;
        }

        throw InvalidKey("an integer", raw);
    }

    private static double ParseRealKey(string raw)
    {
        if (double.TryParse(
                raw,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out double real) &&
            double.IsFinite(real))
        {
            return real;
        }

        throw InvalidKey("a finite real number", raw);
    }

    private static decimal ParseDecimalKey(string raw)
    {
        if (decimal.TryParse(
                raw,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out decimal exact))
        {
            return exact;
        }

        throw InvalidKey("an exact decimal number", raw);
    }

    private static BadHttpRequestException InvalidKey(string expected, string raw) =>
        new(
            $"Primary-key value '{raw}' is not {expected}.",
            StatusCodes.Status400BadRequest);

    private static void ValidateKeyTransportType(string? transportType)
    {
        if (string.IsNullOrWhiteSpace(transportType) ||
            transportType is
                "binary" or
                "boolean" or
                "integer" or
                "real" or
                "decimal" or
                "uuid" or
                "date" or
                "time" or
                "timestamp" or
                "timestamp-with-time-zone" or
                "interval-day-to-second")
        {
            return;
        }

        throw new BadHttpRequestException(
            $"Unsupported primary-key transport type '{transportType}'.",
            StatusCodes.Status400BadRequest);
    }
}
