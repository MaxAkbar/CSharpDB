using CSharpDB.Sql;

namespace CSharpDB.Admin.Services;

/// <summary>
/// Admin-local change notifier used to refresh UI components after client writes.
/// </summary>
public sealed class DatabaseChangeService
{
    private const string ProcedureTableName = "__procedures";

    public event Action? Changed;

    public void NotifyChanged() => Changed?.Invoke();

    public void NotifyFromSql(string sql)
    {
        AnalyzeSqlEffects(sql, out bool schemaMutated, out bool proceduresMutated);
        if (schemaMutated || proceduresMutated)
            NotifyChanged();
    }

    private static void AnalyzeSqlEffects(string sql, out bool schemaMutated, out bool proceduresMutated)
    {
        schemaMutated = false;
        proceduresMutated = false;

        foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(sql))
        {
            if (LooksLikeSchemaMutation(statement))
                schemaMutated = true;

            proceduresMutated |= LooksLikeProcedureMutation(statement);
        }
    }

    private static bool LooksLikeSchemaMutation(string sql)
    {
        string upper = sql.TrimStart().ToUpperInvariant();
        return upper.StartsWith("CREATE ", StringComparison.Ordinal)
            || upper.StartsWith("DROP ", StringComparison.Ordinal)
            || upper.StartsWith("ALTER ", StringComparison.Ordinal);
    }

    private static bool LooksLikeProcedureMutation(string sql)
    {
        string upper = sql.TrimStart().ToUpperInvariant();
        return upper.StartsWith($"INSERT INTO {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"UPDATE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"DELETE FROM {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"CREATE TABLE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"CREATE TABLE IF NOT EXISTS {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"DROP TABLE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"ALTER TABLE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal);
    }
}
