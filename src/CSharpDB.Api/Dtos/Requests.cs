using System.Text.Json;

namespace CSharpDB.Api.Dtos;

// ─── Table / Column ─────────────────────────────────────────

public sealed record RenameTableRequest(string NewName);
public sealed record AddColumnRequest(string ColumnName, string Type, bool NotNull = false);
public sealed record RenameColumnRequest(string NewName);

// ─── Rows ───────────────────────────────────────────────────

public sealed record InsertRowRequest(Dictionary<string, object?> Values);
public sealed record UpdateRowRequest(Dictionary<string, object?> Values);

// ─── Indexes ────────────────────────────────────────────────

public sealed record CreateIndexRequest(string IndexName, string TableName, string ColumnName, bool IsUnique = false);
public sealed record UpdateIndexRequest(string NewIndexName, string TableName, string ColumnName, bool IsUnique = false);

// ─── Views ──────────────────────────────────────────────────

public sealed record CreateViewRequest(string ViewName, string SelectSql);
public sealed record UpdateViewRequest(string NewViewName, string SelectSql);

// ─── Triggers ───────────────────────────────────────────────

public sealed record CreateTriggerRequest(string TriggerName, string TableName, string Timing, string Event, string BodySql);
public sealed record UpdateTriggerRequest(string NewTriggerName, string TableName, string Timing, string Event, string BodySql);

// ─── SQL ────────────────────────────────────────────────────

public sealed record ExecuteSqlRequest(string Sql);

// ─── Saved Queries ──────────────────────────────────────────

public sealed record UpsertSavedQueryRequest(string SqlText);

// ─── Collections ────────────────────────────────────────────

public sealed record PutDocumentRequest(JsonElement Document);
