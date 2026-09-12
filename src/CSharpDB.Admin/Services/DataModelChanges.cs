using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Admin.Models;
using CSharpDB.Client.Models;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Services;

public sealed partial class DataModelService
{
    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    private static string OperationsHash(DataModelState state) => Hash(JsonSerializer.Serialize(state.PendingOperations));
    public static bool MatchesReview(DataModelState state, DataModelChangePlan plan) => OperationsHash(state) == plan.OperationFingerprint;

    // Read through the same transaction as DDL so the drift check cannot race another schema writer.
    private async Task<string> ReadSchemaFingerprintAsync(string? transaction, CancellationToken ct)
    {
        var rows = new List<string> { client.DataSource };
        string[] queries = ["SELECT table_name, schema_id FROM sys.tables",
            "SELECT * FROM sys.columns", "SELECT * FROM sys.foreign_keys", "SELECT * FROM sys.key_constraints",
            "SELECT * FROM sys.check_constraints", "SELECT * FROM sys.indexes", "SELECT * FROM sys.views", "SELECT * FROM sys.triggers"];
        foreach (string sql in queries)
        {
            var result = transaction is null ? await client.ExecuteSqlAsync(sql, ct) : await client.ExecuteInTransactionAsync(transaction, sql, ct);
            ThrowIfSqlError(result);
            rows.Add(sql);
            rows.AddRange((result.Rows ?? []).Select(row => JsonSerializer.Serialize(row)).Order(StringComparer.Ordinal));
        }
        return Hash(string.Join('\n', rows));
    }

    public async Task<DataModelChangePlan> ReviewChangesAsync(DataModelState state, CancellationToken ct = default)
    {
        var steps = new List<DataModelChangeStep>();
        var errors = new List<string>();
        var warnings = new List<string>();
        string fingerprint = "";
        string? transaction = null;
        try
        {
            transaction = (await client.BeginTransactionAsync(ct)).TransactionId;
            fingerprint = await ReadSchemaFingerprintAsync(transaction, ct);
            if (state.SchemaFingerprint is null || state.SchemaFingerprint != fingerprint)
                errors.Add("The database schema baseline is missing or changed. Refresh the model and review the pending changes again.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            errors.Add($"Transactional schema review is unavailable: {ex.Message}");
        }
        finally
        {
            if (transaction is not null) await client.RollbackTransactionAsync(transaction, CancellationToken.None);
        }
        // Canvas membership is visual only: removed/hidden cards must not invalidate staged DDL.
        var validationState = DataModelGraphBuilder.Build(await LoadSourcesAsync(ct), autoLayoutLimit: int.MaxValue);
        var preview = new DataModelSchemaProjection(validationState);
        foreach (var operation in state.PendingOperations)
        {
            try
            {
                ValidateOperation(preview.State, operation);
                string sql = BuildOperationSql(operation);
                _ = Parser.Parse(sql); // Exactly one statement, including expression/type inputs.
                bool destructive = operation.Kind is DataModelPendingOperationKind.DropTable or DataModelPendingOperationKind.DropColumn
                    or DataModelPendingOperationKind.AlterColumnType or DataModelPendingOperationKind.DropPrimaryKey
                    or DataModelPendingOperationKind.DropConstraint or DataModelPendingOperationKind.DropForeignKey;
                steps.Add(new(operation.Id, operation.TableName, sql, destructive));
                var node = FindStateNode(preview.State, operation.TableName);
                if (destructive)
                {
                    warnings.Add($"{operation.TableName}: {operation.Kind} changes or removes existing schema/data.");
                    foreach (var dependency in node?.Dependencies ?? []) warnings.Add($"Known dependency: {dependency.Kind} {dependency.Name}.");
                    foreach (var relationship in preview.State.Relationships.Where(r => r.RightTable.Equals(operation.TableName, StringComparison.OrdinalIgnoreCase)))
                        warnings.Add($"Incoming foreign key: {relationship.LeftTable}.{relationship.ConstraintName}. Review dependent changes first.");
                }
                preview.Apply(operation);
            }
            catch (Exception ex) when (ex is not OperationCanceledException) { errors.Add($"{operation.TableName}: {ex.Message}"); }
        }
        if (steps.Count > 0) warnings.Add("Dependency inspection is not exhaustive. Existing data is validated by the engine during application; no data scans ran during this review.");
        return new(fingerprint, OperationsHash(state), steps, errors, warnings.Distinct().ToArray());
    }

    public async Task<DataModelApplyResult> ApplyReviewedChangesAsync(DataModelState state, DataModelChangePlan plan, CancellationToken ct = default)
    {
        // Snapshot both intent and reviewed SQL before the first await; callers cannot change the executing batch.
        var operations = JsonSerializer.Deserialize<List<DataModelPendingOperation>>(JsonSerializer.Serialize(state.PendingOperations))!;
        var steps = plan.Steps.ToArray();
        if (!plan.CanApply) throw new InvalidOperationException(string.Join(" ", plan.Errors.DefaultIfEmpty("No reviewed changes to apply.")));
        if (OperationsHash(state) != plan.OperationFingerprint || state.SchemaFingerprint != plan.SchemaFingerprint ||
            !state.PendingOperations.Select(operation => BuildOperationSql(operation)).SequenceEqual(plan.Steps.Select(step => step.Sql)))
            throw new InvalidOperationException("The pending changes changed after review. Review them again.");
        string transaction = (await client.BeginTransactionAsync(ct)).TransactionId;
        bool committed = false;
        try
        {
            if (await ReadSchemaFingerprintAsync(transaction, ct) != plan.SchemaFingerprint)
                throw new InvalidOperationException("Database schema changed after review. Refresh and review again.");
            foreach (var step in steps) ThrowIfSqlError(await client.ExecuteInTransactionAsync(transaction, step.Sql, ct));
            await client.CommitTransactionAsync(transaction, ct);
            committed = true;
        }
        finally
        {
            if (!committed) await client.RollbackTransactionAsync(transaction, CancellationToken.None);
        }

        // No in-memory applied mutation occurs until the entire transaction has committed.
        var messages = operations.Select(DescribeOperation).ToList();
        var appliedIds = operations.Select(operation => operation.Id).ToHashSet(StringComparer.Ordinal);
        state.PendingOperations.RemoveAll(operation => appliedIds.Contains(operation.Id));
        try
        {
            foreach (var operation in operations)
            {
                if (operation.Kind == DataModelPendingOperationKind.AddForeignKey)
                {
                    var draft = state.Relationships.FirstOrDefault(relationship => relationship.Id == operation.Id);
                    if (draft is not null) { draft.ConstraintName = ConstraintName(operation); draft.Kind = DataModelRelationshipKind.PhysicalForeignKey; }
                }
                else ApplyOperationToState(state, operation);
            }
            var refreshed = DataModelGraphBuilder.BuildFromDiagramState(await LoadSourcesAsync(ct), state);
            state.Nodes = refreshed.Nodes; state.Relationships = refreshed.Relationships; state.Groups = refreshed.Groups; state.Warnings = refreshed.Warnings;
            state.SchemaContext = refreshed.SchemaContext;
            state.SchemaFingerprint = await ReadSchemaFingerprintAsync(null, ct);
            state.SchemaSnapshotUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
            if (!string.IsNullOrWhiteSpace(state.DiagramName)) await SaveDiagramAsync(state.DiagramName, state, ct);
            return new() { Succeeded = true, Messages = messages };
        }
        catch (Exception ex)
        {
            // A stale saved plan is blocked by its schema fingerprint if reopening follows this failure.
            messages.Add($"Schema changes were committed, but refreshing/saving the diagram failed: {ex.Message}. Do not reapply; refresh the model.");
            return new() { Succeeded = true, DiagramSaved = false, RequiresRefresh = true, Messages = messages };
        }
    }

    private static string ConstraintName(DataModelPendingOperation operation) =>
        string.IsNullOrWhiteSpace(operation.ConstraintName) ? "fk_" + operation.Id.Replace("-", "") : operation.ConstraintName;
    private static IReadOnlyList<string> ChildColumns(DataModelPendingOperation operation) => operation.ColumnNames.Count > 0 ? operation.ColumnNames : [RequireValue(operation.ColumnName, "column name")];
    private static IReadOnlyList<string> ParentColumns(DataModelPendingOperation operation) => operation.ReferencedColumnNames.Count > 0 ? operation.ReferencedColumnNames : [RequireValue(operation.ReferencedColumnName, "referenced column")];
    private static string ColumnList(IEnumerable<string> names) => string.Join(", ", names.Select(FormatIdentifier));
    private static string BuildForeignKeySql(DataModelPendingOperation operation) =>
        $"ALTER TABLE {FormatIdentifier(operation.TableName)} ADD CONSTRAINT {FormatIdentifier(ConstraintName(operation))} FOREIGN KEY ({ColumnList(ChildColumns(operation))}) REFERENCES {FormatIdentifier(RequireValue(operation.ReferencedTableName, "referenced table"))} ({ColumnList(ParentColumns(operation))}) ON DELETE {ActionSql(operation.OnDelete)} ON UPDATE {ActionSql(operation.OnUpdate)};";
    private static string ActionSql(string action) => action.Trim().ToUpperInvariant() switch
    { "RESTRICT" => "RESTRICT", "NO ACTION" => "NO ACTION", "CASCADE" => "CASCADE", "SET NULL" => "SET NULL", "SET DEFAULT" => "SET DEFAULT", _ => throw new InvalidOperationException("Unsupported referential action.") };

    private static string BuildExtendedOperationSql(DataModelPendingOperation operation)
    {
        string table = FormatIdentifier(operation.TableName);
        string column = FormatIdentifier(operation.ColumnName ?? "");
        string alter = $"ALTER TABLE {table} ALTER COLUMN {column}";
        return operation.Kind switch
        {
            DataModelPendingOperationKind.AlterColumnType => $"{alter} TYPE {NormalizeTypeLabel(operation.ColumnType)};",
            DataModelPendingOperationKind.SetDefault => $"{alter} SET DEFAULT {RequireValue(operation.ExpressionSql, "literal default")};",
            DataModelPendingOperationKind.DropDefault => $"{alter} DROP DEFAULT;",
            DataModelPendingOperationKind.SetNotNull => $"{alter} SET NOT NULL;",
            DataModelPendingOperationKind.DropNotNull => $"{alter} DROP NOT NULL;",
            DataModelPendingOperationKind.SetCollation => $"{alter} SET COLLATION {FormatIdentifier(RequireValue(operation.Collation, "collation"))};",
            DataModelPendingOperationKind.DropCollation => $"{alter} DROP COLLATION;",
            DataModelPendingOperationKind.AddPrimaryKey => $"ALTER TABLE {table} ADD CONSTRAINT {FormatIdentifier(RequireValue(operation.ConstraintName, "constraint name"))} PRIMARY KEY ({ColumnList(ChildColumns(operation))});",
            DataModelPendingOperationKind.AddUniqueKey => $"ALTER TABLE {table} ADD CONSTRAINT {FormatIdentifier(RequireValue(operation.ConstraintName, "constraint name"))} UNIQUE ({ColumnList(ChildColumns(operation))});",
            DataModelPendingOperationKind.AddCheck => $"ALTER TABLE {table} ADD CONSTRAINT {FormatIdentifier(RequireValue(operation.ConstraintName, "constraint name"))} CHECK ({RequireValue(operation.ExpressionSql, "check expression")});",
            DataModelPendingOperationKind.DropConstraint => $"ALTER TABLE {table} DROP CONSTRAINT {FormatIdentifier(RequireValue(operation.ConstraintName, "constraint name"))};",
            DataModelPendingOperationKind.DropPrimaryKey => $"ALTER TABLE {table} DROP PRIMARY KEY;",
            DataModelPendingOperationKind.CreateIndex => $"CREATE {(operation.IsUnique ? "UNIQUE " : "")}INDEX {FormatIdentifier(RequireValue(operation.IndexName, "index name"))} ON {table} ({string.Join(", ", ChildColumns(operation).Select((name, i) => FormatIdentifier(name) + (i < operation.ColumnCollations.Count && !string.IsNullOrWhiteSpace(operation.ColumnCollations[i]) ? " COLLATE " + FormatIdentifier(operation.ColumnCollations[i]!) : "")))});",
            DataModelPendingOperationKind.DropIndex => $"DROP INDEX {FormatIdentifier(RequireValue(operation.IndexName, "index name"))};",
            _ => throw new InvalidOperationException("Unsupported modeler operation.")
        };
    }

    private static void ValidateOperation(DataModelState state, DataModelPendingOperation operation, bool canFoldIntoCreate = false)
    {
        var node = FindStateNode(state, operation.TableName);
        if (node?.Kind == DataModelNodeKind.ExternalTable) throw new InvalidOperationException("External tables are read-only.");
        if (operation.Kind == DataModelPendingOperationKind.CreateTable)
        {
            if (node is not null) throw new InvalidOperationException("A table with that name already exists in the pending schema.");
            return;
        }
        if (node is null) throw new InvalidOperationException($"Table '{operation.TableName}' is not present at this step. Check earlier create, rename, or drop operations.");
        if (operation.Kind == DataModelPendingOperationKind.RenameTable && FindStateNode(state, operation.NewTableName ?? "") is not null)
            throw new InvalidOperationException("A table with the new name already exists.");
        if (operation.Kind is DataModelPendingOperationKind.AlterColumnType or DataModelPendingOperationKind.DropColumn or DataModelPendingOperationKind.RenameColumn or
            DataModelPendingOperationKind.SetDefault or DataModelPendingOperationKind.DropDefault or DataModelPendingOperationKind.SetNotNull or DataModelPendingOperationKind.DropNotNull or
            DataModelPendingOperationKind.SetCollation or DataModelPendingOperationKind.DropCollation)
        {
            if (!node.Columns.Any(column => column.Name.Equals(operation.ColumnName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Column '{operation.ColumnName}' is not present at this step. Check earlier add, rename, or drop operations.");
        }
        if (operation.Kind == DataModelPendingOperationKind.RenameColumn && node.Columns.Any(column => column.Name.Equals(operation.NewColumnName, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException("A column with the new name already exists.");
        if (operation.Kind == DataModelPendingOperationKind.DropColumn)
        {
            string name = operation.ColumnName!;
            if (node.Columns.Count == 1) throw new InvalidOperationException("Cannot drop the last column of a table.");
            if (node.Keys.Any(key => key.Columns.Contains(name, StringComparer.OrdinalIgnoreCase)) ||
                node.Indexes.Any(index => index.Columns.Contains(name, StringComparer.OrdinalIgnoreCase)) ||
                node.Checks.Any(check => CSharpDB.Execution.QueryPlanner.CheckReferencesColumn(new()
                { ColumnName = check.ColumnName, ExpressionSql = check.ExpressionSql }, name)) ||
                state.Relationships.Any(r => r.EffectiveColumnPairs.Any(pair =>
                    r.LeftTable.Equals(node.Name, StringComparison.OrdinalIgnoreCase) && pair.ChildColumn.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                    r.RightTable.Equals(node.Name, StringComparison.OrdinalIgnoreCase) && pair.ParentColumn.Equals(name, StringComparison.OrdinalIgnoreCase))))
                throw new InvalidOperationException("Remove dependent keys, indexes, checks, and foreign keys before dropping this column.");
        }
        var changedColumn = node.Columns.FirstOrDefault(column => column.Name.Equals(operation.ColumnName, StringComparison.OrdinalIgnoreCase));
        if (operation.Kind == DataModelPendingOperationKind.DropNotNull && changedColumn is { } required && (required.IsPrimaryKey || required.IsIdentity))
            throw new InvalidOperationException("Primary-key and identity columns cannot be made nullable.");
        if (operation.Kind == DataModelPendingOperationKind.SetCollation)
            CSharpDB.Execution.QueryPlanner.ValidateColumnCollation(changedColumn!.Name,
                ((CreateTableStatement)Parser.Parse($"CREATE TABLE probe (value {NormalizeTypeLabel(changedColumn.TypeLabel)});")).Columns[0].DeclaredType, operation.Collation);
        if (operation.Kind == DataModelPendingOperationKind.AddPrimaryKey && node.Keys.Any(key => key.Kind == KeyConstraintKind.PrimaryKey))
            throw new InvalidOperationException("Remove the existing primary key before adding its replacement.");
        if (operation.Kind == DataModelPendingOperationKind.CreateIndex && state.Nodes.Any(table => table.Indexes.Any(index => index.IndexName.Equals(operation.IndexName, StringComparison.OrdinalIgnoreCase))))
            throw new InvalidOperationException("An index with that name already exists.");
        if (operation.Kind is DataModelPendingOperationKind.AddPrimaryKey or DataModelPendingOperationKind.AddUniqueKey or DataModelPendingOperationKind.AddCheck or DataModelPendingOperationKind.AddForeignKey)
            if (!string.IsNullOrWhiteSpace(operation.ConstraintName) && (node.Keys.Any(key => key.ConstraintName?.Equals(operation.ConstraintName, StringComparison.OrdinalIgnoreCase) == true) ||
                node.Checks.Any(check => check.ConstraintName?.Equals(operation.ConstraintName, StringComparison.OrdinalIgnoreCase) == true) ||
                state.Relationships.Any(r => r.LeftTable.Equals(node.Name, StringComparison.OrdinalIgnoreCase) && r.ConstraintName?.Equals(operation.ConstraintName, StringComparison.OrdinalIgnoreCase) == true)))
                throw new InvalidOperationException("A constraint with that name already exists. Remove it before adding its replacement.");
        if (operation.Kind == DataModelPendingOperationKind.AddColumn)
        {
            if (node?.Columns.Any(column => column.Name.Equals(operation.ColumnName, StringComparison.OrdinalIgnoreCase)) == true)
                throw new InvalidOperationException("A column with that name already exists.");
            if (NormalizeTypeLabel(operation.ColumnType) == "ROWVERSION" && !canFoldIntoCreate)
                throw new InvalidOperationException("ROWVERSION can only be declared when creating a table. Add it to a new draft before staging other edits for that table.");
            if (NormalizeTypeLabel(operation.ColumnType) == "ROWVERSION" &&
                (operation.NotNull || !string.IsNullOrWhiteSpace(operation.ExpressionSql) || !string.IsNullOrWhiteSpace(operation.Collation) || !string.IsNullOrWhiteSpace(operation.CheckExpressionSql)))
                throw new InvalidOperationException("ROWVERSION is generated and cannot have nullability, default, collation, or check modifiers.");
        }
        if (operation.Kind == DataModelPendingOperationKind.SetDefault || operation.Kind == DataModelPendingOperationKind.AddColumn && !string.IsNullOrWhiteSpace(operation.ExpressionSql))
            SchemaColumnRules.ValidateLiteralDefault(RequireValue(operation.ExpressionSql, "literal default"));
        if (operation.Kind is DataModelPendingOperationKind.AddPrimaryKey or DataModelPendingOperationKind.AddUniqueKey or DataModelPendingOperationKind.CreateIndex or DataModelPendingOperationKind.AddForeignKey)
        {
            var columns = ChildColumns(operation);
            if (columns.Count != columns.Distinct(StringComparer.OrdinalIgnoreCase).Count()) throw new InvalidOperationException("A column may appear only once in a key or index.");
            foreach (string name in columns)
            {
                var column = node?.Columns.FirstOrDefault(column => column.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                if (column is null) throw new InvalidOperationException($"Column '{name}' is not on this table.");
                var storage = ((CreateTableStatement)Parser.Parse($"CREATE TABLE probe (value {NormalizeTypeLabel(column.TypeLabel)}); ")).Columns[0].DeclaredType.StorageType;
                if (column.IsRowVersion || storage is not (CSharpDB.Primitives.DbType.Integer or CSharpDB.Primitives.DbType.Real or CSharpDB.Primitives.DbType.Decimal or CSharpDB.Primitives.DbType.Text or CSharpDB.Primitives.DbType.Blob))
                    throw new InvalidOperationException($"The engine does not support {column.TypeLabel} in this key/index operation.");
            }
        }
        if (operation.Kind == DataModelPendingOperationKind.DropIndex && node?.Indexes.Any(index => index.IndexName == operation.IndexName && index.IsEngineManaged) == true)
            throw new InvalidOperationException("Engine-maintained indexes cannot be edited directly. Edit the owning constraint instead.");
        if (operation.Kind == DataModelPendingOperationKind.AddForeignKey)
        {
            var parent = FindStateNode(state, operation.ReferencedTableName ?? "");
            if (parent is null) throw new InvalidOperationException("The referenced table must exist at this step. Check earlier create, rename, or drop operations.");
            if (parent?.Kind == DataModelNodeKind.ExternalTable) throw new InvalidOperationException("An external table cannot be a physical foreign-key target.");
            if (ChildColumns(operation).Count != ParentColumns(operation).Count) throw new InvalidOperationException("Child and parent column lists must have the same length and order.");
            var referenced = ParentColumns(operation);
            if (referenced.Distinct(StringComparer.OrdinalIgnoreCase).Count() != referenced.Count)
                throw new InvalidOperationException("A referenced column may appear only once.");
            foreach (var pair in ChildColumns(operation).Zip(referenced))
            {
                var target = parent!.Columns.FirstOrDefault(column => column.Name.Equals(pair.Second, StringComparison.OrdinalIgnoreCase));
                var child = node!.Columns.First(column => column.Name.Equals(pair.First, StringComparison.OrdinalIgnoreCase));
                if (target is null || NormalizeTypeLabel(target.TypeLabel) != NormalizeTypeLabel(child.TypeLabel))
                    throw new InvalidOperationException($"Referenced column '{pair.Second}' must exist and have the same declared type as '{pair.First}'.");
            }
            var candidates = parent!.Keys.Select(key => key.Columns)
                .Concat(parent.Indexes.Where(index => index.IsUnique).Select(index => index.Columns))
                .Concat(state.PendingOperations.Where(op => op.TableName.Equals(parent.Name, StringComparison.OrdinalIgnoreCase) &&
                    (op.Kind is DataModelPendingOperationKind.AddPrimaryKey or DataModelPendingOperationKind.AddUniqueKey || op.Kind == DataModelPendingOperationKind.CreateIndex && op.IsUnique)).Select(ChildColumns));
            if (!candidates.Any(columns => columns.SequenceEqual(referenced, StringComparer.OrdinalIgnoreCase)) &&
                !(referenced.Count == 1 && parent.Columns.Count(column => column.IsPrimaryKey) == 1 && parent.Columns.Any(column => column.IsPrimaryKey && column.Name.Equals(referenced[0], StringComparison.OrdinalIgnoreCase))))
                throw new InvalidOperationException("Referenced columns must match a complete, ordered primary or unique candidate key.");
            foreach (string name in ChildColumns(operation))
            {
                var column = node?.Columns.FirstOrDefault(column => column.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                if (!DataModelRelationshipRules.IsActionCompatible(operation.OnDelete, column) || !DataModelRelationshipRules.IsActionCompatible(operation.OnUpdate, column))
                    throw new InvalidOperationException($"Referential action is incompatible with {name}'s nullability/default.");
            }
        }
        if (node?.Columns.FirstOrDefault(column => column.Name == operation.ColumnName)?.IsRowVersion == true &&
            operation.Kind is not (DataModelPendingOperationKind.RenameColumn or DataModelPendingOperationKind.DropColumn))
            throw new InvalidOperationException("Generated ROWVERSION properties cannot be altered.");
        if (operation.Kind == DataModelPendingOperationKind.AlterColumnType && NormalizeTypeLabel(operation.ColumnType) == "ROWVERSION")
            throw new InvalidOperationException("ROWVERSION is not an ALTER COLUMN TYPE target.");
    }

    public static string ValidateForStaging(DataModelState state, DataModelPendingOperation operation)
    {
        ValidateBatchForStaging(state, [operation]);
        return BuildOperationSql(operation);
    }

    public static void ValidateBatchForStaging(DataModelState state, IReadOnlyList<DataModelPendingOperation> operations)
    {
        var preview = DataModelSchemaProjection.ForEditing(state);
        if (preview.Errors.Count > 0) throw new InvalidOperationException("Fix or remove invalid pending changes first: " + string.Join(" ", preview.Errors));
        var earlier = state.PendingOperations.ToList();
        foreach (var operation in operations)
        {
            bool canFoldIntoCreate = preview.Find(operation.TableName)?.IsDraft == true &&
                earlier.Any(op => op.TableName.Equals(operation.TableName, StringComparison.OrdinalIgnoreCase) && op.Kind == DataModelPendingOperationKind.CreateTable) &&
                !earlier.Any(op => op.TableName.Equals(operation.TableName, StringComparison.OrdinalIgnoreCase) && op.Kind != DataModelPendingOperationKind.CreateTable);
            ValidateOperation(preview.State, operation, canFoldIntoCreate);
            _ = Parser.Parse(BuildOperationSql(operation));
            preview.Apply(operation);
            earlier.Add(operation);
        }
    }


}
