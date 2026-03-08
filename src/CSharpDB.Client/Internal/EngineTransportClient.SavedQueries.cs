using System.Globalization;
using CSharpDB.Client.Models;
using CSharpDB.Engine;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    public async Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            var result = await ExecuteQueryAsync(
                db,
                $"""
                SELECT id, name, sql_text, created_utc, updated_utc
                FROM {SavedQueryTableName}
                ORDER BY name;
                """,
                ct);

            return (result.Rows ?? [])
                .Select(ReadSavedQueryDefinition)
                .ToArray();
        }
        finally { _lock.Release(); }
    }

    public async Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            return await GetSavedQueryInternalAsync(db, NormalizeSavedQueryName(name), ct);
        }
        finally { _lock.Release(); }
    }

    public async Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            string normalizedName = NormalizeSavedQueryName(name);
            string normalizedSql = NormalizeSqlFragment(sqlText, "saved query SQL");

            var existing = await GetSavedQueryInternalAsync(db, normalizedName, ct);
            string nowText = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);

            if (existing is null)
            {
                await ExecuteStatementAsync(
                    db,
                    $"""
                    INSERT INTO {SavedQueryTableName} (name, sql_text, created_utc, updated_utc)
                    VALUES ({FormatSqlLiteral(normalizedName)}, {FormatSqlLiteral(normalizedSql)}, {FormatSqlLiteral(nowText)}, {FormatSqlLiteral(nowText)});
                    """,
                    ct);
            }
            else
            {
                await ExecuteStatementAsync(
                    db,
                    $"""
                    UPDATE {SavedQueryTableName}
                    SET sql_text = {FormatSqlLiteral(normalizedSql)},
                        updated_utc = {FormatSqlLiteral(nowText)}
                    WHERE name = {FormatSqlLiteral(normalizedName)};
                    """,
                    ct);
            }

            return await GetSavedQueryInternalAsync(db, normalizedName, ct)
                ?? throw new InvalidOperationException($"Saved query '{normalizedName}' could not be loaded after save.");
        }
        finally { _lock.Release(); }
    }

    public async Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            string normalizedName = NormalizeSavedQueryName(name);
            int affected = await ExecuteNonQueryAsync(
                db,
                $"DELETE FROM {SavedQueryTableName} WHERE name = {FormatSqlLiteral(normalizedName)};",
                ct);

            if (affected == 0)
                throw new ArgumentException($"Saved query '{normalizedName}' not found.");
        }
        finally { _lock.Release(); }
    }

    private static async Task<SavedQueryDefinition?> GetSavedQueryInternalAsync(Database db, string normalizedName, CancellationToken ct)
    {
        var result = await ExecuteQueryAsync(
            db,
            $"""
            SELECT id, name, sql_text, created_utc, updated_utc
            FROM {SavedQueryTableName}
            WHERE name = {FormatSqlLiteral(normalizedName)};
            """,
            ct);

        return result.Rows is { Count: > 0 } ? ReadSavedQueryDefinition(result.Rows[0]) : null;
    }

    private static SavedQueryDefinition ReadSavedQueryDefinition(object?[] row)
    {
        long id = Convert.ToInt64(row[0], CultureInfo.InvariantCulture);
        return new SavedQueryDefinition
        {
            Id = id,
            Name = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
            SqlText = Convert.ToString(row[2], CultureInfo.InvariantCulture) ?? string.Empty,
            CreatedUtc = ParseStoredUtc(Convert.ToString(row[3], CultureInfo.InvariantCulture) ?? string.Empty, DateTime.UtcNow),
            UpdatedUtc = ParseStoredUtc(Convert.ToString(row[4], CultureInfo.InvariantCulture) ?? string.Empty, DateTime.UtcNow),
        };
    }
}
