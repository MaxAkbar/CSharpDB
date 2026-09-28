using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using static CSharpDB.DataPrivacy.PrivacyValues;

namespace CSharpDB.DataPrivacy;

internal sealed class PrivacyTable
{
    internal required TableSchema Schema { get; init; }
    internal required TransactionTableSnapshot Metadata { get; init; }
    internal List<string> IdentityColumns { get; set; } = [];
    internal List<Dictionary<string, object?>> Rows { get; } = [];
    internal string Identity(Dictionary<string, object?> row) => Key(Schema, IdentityColumns, row)!;
}

internal sealed class PrivacyPlanner(PrivacyPolicy policy, Dictionary<string, PrivacyTable> tables, PrivacyLimits limits,
    DateTimeOffset reference, CancellationToken ct)
{
    private readonly Dictionary<string, Dictionary<string, List<Dictionary<string, object?>>>> _lookups = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _warnings = [];
    private long _bytes;
    private PrivacyTable Table(string name) => tables.GetValueOrDefault(name) ?? throw new PrivacyException("A configured table no longer exists.");
    private PrivacyRelationship Relation(string id) => policy.Relationships.SingleOrDefault(r => r.Id == id) ?? throw new PrivacyException("A relationship mapping is missing.");
    internal static TableSchema Map(CSharpDB.Client.Models.TableSchema schema)
        => JsonSerializer.Deserialize<TableSchema>(JsonSerializer.Serialize(schema, PrivacyPolicy.JsonOptions), PrivacyPolicy.JsonOptions)!;

    internal PrivacyPreview Build(object identity, string fingerprint)
    {
        Validate();
        _bytes = tables.Values.Sum(t => t.Rows.Sum(r => Size(r.Values) + 128L));
        var preview = new PrivacyPreview { Policy = policy, ClientIdentity = identity, Fingerprint = fingerprint, ReferenceUtc = reference };
        var root = Table(policy.RootTable);
        var flags = new Dictionary<Dictionary<string, object?>, int>();
        foreach (var row in root.Rows)
        {
            ct.ThrowIfCancellationRequested();
            bool eligible = Matches(policy.Eligibility, root, row) == true;
            flags[row] = eligible ? 1 : 2;
            if (eligible) preview.EligibleRecords++;
        }
        var reports = new List<PrivacyTablePreview>();
        foreach (var target in policy.Targets)
        {
            ct.ThrowIfCancellationRequested();
            var table = Table(target.Table);
            var owners = flags;
            foreach (var edge in target.RelationshipPath)
            {
                var next = new Dictionary<Dictionary<string, object?>, int>();
                var relation = Relation(edge);
                foreach (var (row, ownersFlag) in owners)
                {
                    ct.ThrowIfCancellationRequested();
                    foreach (var related in Related(relation, row)) next[related] = next.GetValueOrDefault(related) | ownersFlag;
                }
                owners = next;
            }
            if (owners.Any(p => p.Value == 3)) throw new PrivacyException($"{target.Table}: a selected row is shared with an ineligible main record. Correct the mapping or eligibility.");
            var final = new Dictionary<Dictionary<string, object?>, Dictionary<string, object?>>();
            var samples = new List<PrivacySample>();
            int eligibleCount = 0, changedCount = 0;
            foreach (var (before, owner) in owners)
            {
                ct.ThrowIfCancellationRequested();
                if (owner != 1) continue;
                eligibleCount++;
                var after = new Dictionary<string, object?>(before, StringComparer.OrdinalIgnoreCase);
                string rowIdentity = table.Identity(before);
                foreach (var mask in target.Columns) after[mask.Column] = Mask(policy, table.Schema, rowIdentity, mask, before[mask.Column]);
                if (target.Columns.All(m => Equal(before[m.Column], after[m.Column]))) continue;
                changedCount++;
                AddBytes(Size(after.Values) + 128);
                if (preview.Changes.Count >= limits.MaxAffectedRows) throw new PrivacyException("The affected-row limit was exceeded. Narrow the eligibility rules.");
                final[before] = after;
                preview.Changes.Add(new(target.Table, rowIdentity, before, after));
                if (samples.Count < limits.PreviewRows)
                {
                    var sample = new PrivacySample(target.Columns.ToDictionary(m => m.Column, m => Display(before[m.Column])), target.Columns.ToDictionary(m => m.Column, m => Display(after[m.Column])));
                    AddBytes(Size(sample.Before.Values) + Size(sample.After.Values)); samples.Add(sample);
                }
            }
            ValidateFinal(table, final);
            reports.Add(new(target.Table, eligibleCount, changedCount,
                table.Schema.Columns.Where(c => !c.IsRowVersion && !target.Columns.Any(m => Same(m.Column, c.Name))).Select(c => c.Name).ToArray(), samples));
        }
        preview.Tables = reports;
        preview.Warnings = _warnings.Order().ToArray();
        return preview;
    }

    private void AddBytes(long bytes)
    {
        _bytes += bytes;
        if (_bytes > limits.MaxPreparedBytes) throw new PrivacyException("Privacy preparation exceeded its memory limit. Narrow the policy or use a smaller database.");
    }

    private void Validate()
    {
        if (policy.Version != 1 || policy.Id == Guid.Empty || string.IsNullOrWhiteSpace(policy.Name) || policy.Name.Length > 200)
            throw new PrivacyException("A valid policy name, identity and version are required.");
        if (policy.Targets.Count == 0 || policy.Targets.Count > 64 || policy.Relationships.Count > 128)
            throw new PrivacyException("Select 1–64 target tables and at most 128 mappings.");
        if (policy.Targets.Select(t => t.Table).Distinct(StringComparer.OrdinalIgnoreCase).Count() != policy.Targets.Count
            || policy.Relationships.Select(r => r.Id).Distinct().Count() != policy.Relationships.Count)
            throw new PrivacyException("Target tables and relationship identifiers must be unique.");
        foreach (var relation in policy.Relationships)
        {
            if (relation.SourceColumns.Count == 0 || relation.SourceColumns.Count != relation.TargetColumns.Count
                || relation.SourceColumns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != relation.SourceColumns.Count
                || relation.TargetColumns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != relation.TargetColumns.Count)
                throw new PrivacyException("Relationship mappings require equal, nonempty ordered column lists.");
            for (int i = 0; i < relation.SourceColumns.Count; i++)
            {
                var source = Column(Table(relation.SourceTable).Schema, relation.SourceColumns[i]);
                var target = Column(Table(relation.TargetTable).Schema, relation.TargetColumns[i]);
                if (source.Type != target.Type) throw new PrivacyException("Relationship column storage types must match.");
            }
        }
        int conditions = 0;
        void ValidateCondition(PrivacyCondition condition, string tableName, int depth)
        {
            if (++conditions > 256 || depth > 12 || !Enum.IsDefined(condition.Kind)) throw new PrivacyException("Invalid or overly complex eligibility conditions.");
            var table = Table(tableName);
            if (condition.Kind is PrivacyConditionKind.Exists or PrivacyConditionKind.NotExists)
            {
                var relation = Relation(condition.RelationshipId);
                if (!Same(relation.SourceTable, tableName)) throw new PrivacyException("A related condition must start at its current table.");
                foreach (var child in condition.Children) ValidateCondition(child, relation.TargetTable, depth + 1);
            }
            else if (condition.Kind is PrivacyConditionKind.All or PrivacyConditionKind.Any)
            {
                if (condition.Children.Count == 0) throw new PrivacyException("Add at least one eligibility condition before previewing.");
                foreach (var child in condition.Children) ValidateCondition(child, tableName, depth + 1);
            }
            else
            {
                if (condition.Children.Count > 0) throw new PrivacyException("Only groups and related conditions can contain child conditions.");
                var column = Column(table.Schema, condition.Column);
                if (condition.Kind == PrivacyConditionKind.Compare)
                {
                    if (!Enum.IsDefined(condition.Comparison)) throw new PrivacyException("Unknown comparison.");
                    _ = Constant(condition.Value, column, tableName);
                }
                if (condition.Kind is PrivacyConditionKind.OlderThan or PrivacyConditionKind.WithinLast)
                {
                    if (condition.Days is < 1 or > 365000 || !Enum.IsDefined(condition.DateEncoding)) throw new PrivacyException("Retention days must be between 1 and 365000.");
                    if (condition.DateEncoding == PrivacyDateEncoding.ExactFormat)
                    {
                        if (string.IsNullOrWhiteSpace(condition.DateFormat)) throw new PrivacyException("An explicit date format is required.");
                        try { _ = TimeZoneInfo.FindSystemTimeZoneById(condition.TimeZoneId); }
                        catch { throw new PrivacyException("Unknown date time-zone identifier."); }
                    }
                }
            }
        }
        ValidateCondition(policy.Eligibility, policy.RootTable, 0);
        foreach (var target in policy.Targets)
        {
            var table = Table(target.Table);
            string current = policy.RootTable;
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { current };
            foreach (string step in target.RelationshipPath)
            {
                var r = Relation(step);
                if (!Same(r.SourceTable, current) || !visited.Add(r.TargetTable)) throw new PrivacyException("Target paths must be connected and cannot contain cycles.");
                current = r.TargetTable;
            }
            if (!Same(current, target.Table)) throw new PrivacyException("A target's relationship path must connect the main table to that target.");
            if (table.IdentityColumns.Count == 0) throw new PrivacyException($"{target.Table}: a non-null primary or unique key is required to safely locate records.");
            if (table.Metadata.Triggers.Any(t => t.Event == CSharpDB.Client.Models.TriggerEvent.Update)
                || table.Metadata.HasUpdateHostCallbacks != false)
                throw new PrivacyException($"{target.Table}: UPDATE triggers or unverified host callbacks prevent privacy updates.");
            if (target.Columns.Count == 0 || target.Columns.Select(c => c.Column).Distinct(StringComparer.OrdinalIgnoreCase).Count() != target.Columns.Count)
                throw new PrivacyException("Select distinct columns to mask in each target table.");
            var protectedColumns = table.IdentityColumns.Concat(table.Schema.Columns.Where(c => c.IsPrimaryKey || c.IsIdentity || c.IsRowVersion).Select(c => c.Name))
                .Concat(table.Schema.KeyConstraints.Where(k => k.Kind == KeyConstraintKind.PrimaryKey).SelectMany(k => k.Columns))
                .Concat(table.Schema.ForeignKeys.SelectMany(f => f.ColumnNames.Count > 0 ? f.ColumnNames : [f.ColumnName]))
                .Concat(tables.Values.SelectMany(t => t.Schema.ForeignKeys).Where(f => Same(f.ReferencedTableName, target.Table)).SelectMany(f => f.ReferencedColumnNames.Count > 0 ? f.ReferencedColumnNames : [f.ReferencedColumnName]))
                .Concat(policy.Relationships.Where(r => Same(r.SourceTable, target.Table)).SelectMany(r => r.SourceColumns))
                .Concat(policy.Relationships.Where(r => Same(r.TargetTable, target.Table)).SelectMany(r => r.TargetColumns)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (var mask in target.Columns)
            {
                var column = Column(table.Schema, mask.Column);
                if (protectedColumns.Contains(mask.Column))
                {
                    var dependencies = new List<string>();
                    if (table.IdentityColumns.Contains(mask.Column, StringComparer.OrdinalIgnoreCase) || column.IsPrimaryKey) dependencies.Add("record identity key");
                    if (column.IsIdentity || column.IsRowVersion) dependencies.Add("engine-generated column");
                    foreach (var owner in tables.Values)
                        foreach (var fk in owner.Schema.ForeignKeys)
                            if ((Same(owner.Schema.TableName, target.Table) && (fk.ColumnNames.Count > 0 ? fk.ColumnNames : [fk.ColumnName]).Contains(mask.Column, StringComparer.OrdinalIgnoreCase))
                                || (Same(fk.ReferencedTableName, target.Table) && (fk.ReferencedColumnNames.Count > 0 ? fk.ReferencedColumnNames : [fk.ReferencedColumnName]).Contains(mask.Column, StringComparer.OrdinalIgnoreCase)))
                                dependencies.Add($"relationship {owner.Schema.TableName}.{fk.ConstraintName}");
                    foreach (var relation in policy.Relationships)
                        if ((Same(relation.SourceTable, target.Table) && relation.SourceColumns.Contains(mask.Column, StringComparer.OrdinalIgnoreCase))
                            || (Same(relation.TargetTable, target.Table) && relation.TargetColumns.Contains(mask.Column, StringComparer.OrdinalIgnoreCase)))
                            dependencies.Add($"configured relationship {relation.SourceTable} → {relation.TargetTable} ({relation.Id})");
                    throw new PrivacyException($"{target.Table}.{mask.Column}: must be preserved because it participates in {string.Join("; ", dependencies)}.");
                }
                if (!Enum.IsDefined(mask.Kind)) throw new PrivacyException("Unknown masking operation.");
                if (mask.Kind == PrivacyMaskKind.Erase && !column.Nullable) throw new PrivacyException($"{target.Table}.{mask.Column}: NULL is not allowed.");
                if (mask.Kind == PrivacyMaskKind.Constant) _ = Constant(mask.Replacement, column, target.Table);
                if (mask.Kind is PrivacyMaskKind.Anonymous or PrivacyMaskKind.Partial or PrivacyMaskKind.Email && column.Type != DbType.Text)
                    throw new PrivacyException("Anonymous, partial and email masks require a text column.");
                if (mask.KeepPrefix < 0 || mask.KeepSuffix < 0) throw new PrivacyException("Retained prefix and suffix cannot be negative.");
            }
        }
    }

    private IEnumerable<Dictionary<string, object?>> Related(PrivacyRelationship relation, Dictionary<string, object?> row)
    {
        var target = Table(relation.TargetTable);
        if (!_lookups.TryGetValue(relation.Id, out var lookup))
        {
            lookup = new(StringComparer.Ordinal);
            foreach (var targetRow in target.Rows)
            {
                ct.ThrowIfCancellationRequested();
                string? key = Key(target.Schema, relation.TargetColumns, targetRow);
                if (key is null) continue;
                if (!lookup.TryGetValue(key, out var bucket)) { lookup[key] = bucket = []; AddBytes(key.Length * 2L + 128); }
                bucket.Add(targetRow); AddBytes(16);
            }
            _lookups[relation.Id] = lookup;
        }
        var mapped = relation.TargetColumns.Select((name, i) => (name, value: row[relation.SourceColumns[i]]))
            .ToDictionary(p => p.name, p => p.value, StringComparer.OrdinalIgnoreCase);
        string? sourceKey = Key(target.Schema, relation.TargetColumns, mapped);
        return sourceKey is not null && lookup.TryGetValue(sourceKey, out var rows) ? rows : [];
    }

    // Unknown dates cannot prove the absence of recent activity. Preserve unknown through
    // groups and related predicates so NOT EXISTS cannot turn invalid data into eligibility.
    private static bool? All(IEnumerable<bool?> values)
    {
        bool unknown = false;
        foreach (bool? value in values) { if (value == false) return false; unknown |= value is null; }
        return unknown ? null : true;
    }
    private static bool? Any(IEnumerable<bool?> values)
    {
        bool unknown = false;
        foreach (bool? value in values) { if (value == true) return true; unknown |= value is null; }
        return unknown ? null : false;
    }
    private bool? Matches(PrivacyCondition c, PrivacyTable table, Dictionary<string, object?> row)
    {
        ct.ThrowIfCancellationRequested();
        if (c.Kind == PrivacyConditionKind.All) return All(c.Children.Select(child => Matches(child, table, row)));
        if (c.Kind == PrivacyConditionKind.Any) return Any(c.Children.Select(child => Matches(child, table, row)));
        if (c.Kind is PrivacyConditionKind.Exists or PrivacyConditionKind.NotExists)
        {
            var r = Relation(c.RelationshipId);
            bool? exists = Any(Related(r, row).Select(related => All(c.Children.Select(child => Matches(child, Table(r.TargetTable), related)))));
            return c.Kind == PrivacyConditionKind.Exists ? exists : !exists;
        }
        object? value = row[c.Column];
        if (c.Kind == PrivacyConditionKind.IsNull) return value is null;
        if (c.Kind == PrivacyConditionKind.IsNotNull) return value is not null;
        if (value is null) return null;
        if (c.Kind is PrivacyConditionKind.OlderThan or PrivacyConditionKind.WithinLast)
        {
            if (!TryDate(value, c, out var date)) { _warnings.Add($"{table.Schema.TableName}.{c.Column}: invalid or ambiguous dates were excluded from age comparisons; they cannot establish absence of recent activity."); return null; }
            var cutoff = reference.AddDays(-c.Days);
            return c.Kind == PrivacyConditionKind.OlderThan ? date < cutoff : date >= cutoff && date <= reference;
        }
        var column = Column(table.Schema, c.Column);
        int comparison = CollationSupport.Compare(Db(value), Db(Constant(c.Value, column, table.Schema.TableName)), column.Collation);
        return c.Comparison switch
        {
            PrivacyComparison.Equal => comparison == 0, PrivacyComparison.NotEqual => comparison != 0,
            PrivacyComparison.Less => comparison < 0, PrivacyComparison.LessOrEqual => comparison <= 0,
            PrivacyComparison.Greater => comparison > 0, PrivacyComparison.GreaterOrEqual => comparison >= 0,
            _ => false
        };
    }

    private void ValidateFinal(PrivacyTable table, Dictionary<Dictionary<string, object?>, Dictionary<string, object?>> final)
    {
        var unique = table.Schema.KeyConstraints.Select(k => (Columns: k.Columns, Collations: (IReadOnlyList<string?>?)null))
            .Concat(table.Metadata.Indexes.Where(i => i.IsUnique).Select(i => (Columns: i.Columns, Collations: i.ColumnCollations.Count == i.Columns.Count ? i.ColumnCollations : null))).ToList();
        if (table.Schema.Columns.Any(c => c.IsPrimaryKey)) unique.Add((table.Schema.Columns.Where(c => c.IsPrimaryKey).Select(c => c.Name).ToArray(), null));
        foreach (var key in unique)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var before in table.Rows)
            {
                ct.ThrowIfCancellationRequested();
                string? value = Key(table.Schema, key.Columns, final.GetValueOrDefault(before) ?? before, key.Collations);
                if (value is not null && !seen.Add(value)) throw new PrivacyException($"{table.Schema.TableName}: replacements would violate a unique constraint.");
            }
        }
        var checks = table.Schema.CheckConstraints.Select(c => SafeCheck(c.ExpressionSql)).ToArray();
        foreach (var after in final.Values)
            foreach (var check in checks)
            {
                ct.ThrowIfCancellationRequested();
                var valid = ExpressionEvaluator.Evaluate(check, table.Schema.Columns.Select(c => Db(after[c.Name])).ToArray(), table.Schema);
                if (!valid.IsNull && !valid.IsTruthy) throw new PrivacyException($"{table.Schema.TableName}: a replacement violates a CHECK constraint.");
            }
    }
}
