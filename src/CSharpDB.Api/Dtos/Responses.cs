namespace CSharpDB.Api.Dtos;

// ─── Column / Schema ────────────────────────────────────────

public sealed record ColumnResponse(string Name, string Type, bool Nullable, bool IsPrimaryKey, bool IsIdentity, string? Collation);

public sealed record TableSchemaResponse(
    string TableName,
    IReadOnlyList<ColumnResponse> Columns);

// ─── Browse ─────────────────────────────────────────────────

public sealed record BrowseResponse(
    string[] ColumnNames,
    IReadOnlyList<Dictionary<string, object?>> Rows,
    int TotalRows,
    int Page,
    int PageSize,
    int TotalPages);

// ─── Counts / Mutations ─────────────────────────────────────

public sealed record RowCountResponse(string TableName, int Count);
public sealed record MutationResponse(int RowsAffected);
public sealed record CollectionCountResponse(string CollectionName, int Count);

// ─── Indexes ────────────────────────────────────────────────

public sealed record IndexResponse(string IndexName, string TableName, IReadOnlyList<string> Columns, bool IsUnique, IReadOnlyList<string?> ColumnCollations);

// ─── Views ──────────────────────────────────────────────────

public sealed record ViewResponse(string ViewName, string Sql);

// ─── Triggers ───────────────────────────────────────────────

public sealed record TriggerResponse(string TriggerName, string TableName, string Timing, string Event, string BodySql);

// ─── SQL ────────────────────────────────────────────────────

public sealed record SqlResultResponse(
    bool IsQuery,
    string[]? ColumnNames,
    IReadOnlyList<Dictionary<string, object?>>? Rows,
    int RowsAffected,
    string? Error,
    double ElapsedMs);

// ─── Database Info ──────────────────────────────────────────

public sealed record DatabaseInfoResponse(
    string DataSource,
    int TableCount,
    int IndexCount,
    int ViewCount,
    int TriggerCount,
    int ProcedureCount,
    int CollectionCount = 0,
    int SavedQueryCount = 0);
