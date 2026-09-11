using CSharpDB.Sql;

namespace CSharpDB.Admin.Services;

/// <summary>
/// Admin-local change notifier used to refresh UI components after client writes.
/// </summary>
public sealed class DatabaseChangeService
{
    private static readonly HashSet<string> DefinitionTables = new(StringComparer.OrdinalIgnoreCase)
    { "__procedures", "__saved_queries", "__forms", "__reports", "__report_definition_chunks", "__code_modules", "__validation_rules", "__data_model_diagrams", "__external_tables", "_etl_pipelines", "_etl_pipeline_versions" };

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
        try
        {
            string? target = Parser.Parse(sql) switch { InsertStatement insert => insert.TableName, UpdateStatement update => update.TableName, DeleteStatement delete => delete.TableName, _ => null };
            return target is not null && DefinitionTables.Contains(target);
        }
        catch (CSharpDB.Primitives.CSharpDbException) { return false; }
    }
}
