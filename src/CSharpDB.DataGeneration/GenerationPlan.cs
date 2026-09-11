using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.DataGeneration;

/// <summary>An immutable resolved run: full validation streams rows, retaining only bounded key pools.</summary>
public sealed class GenerationPlan
{
    private readonly GenerationProfile _profile;
    private readonly Dictionary<string, PlannedTable> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, GenerationTableSnapshot> _snapshots;
    private readonly GenerationLimits _limits;
    private long _keyBytes;
    public string ProfileHash => _profile.Hash;
    public string ProfileJson => _profile.ToJson();
    public int Seed => _profile.Seed;
    public DateTime ReferenceUtc => _profile.ReferenceUtc;
    public IReadOnlyList<string> Order { get; private set; } = [];
    public IReadOnlyDictionary<string, string> SnapshotFingerprints => _snapshots.ToDictionary(p => p.Key, p => p.Value.Fingerprint, StringComparer.OrdinalIgnoreCase);
    public IReadOnlyDictionary<string, int> Counts => _tables.ToDictionary(p => p.Key, p => p.Value.Rule.Rows, StringComparer.OrdinalIgnoreCase);
    public long GeneratedBytes { get; private set; }

    private GenerationPlan(GenerationProfile profile, IReadOnlyList<GenerationTableSnapshot> snapshots, GenerationLimits limits)
    {
        _profile = GenerationProfile.FromJson(profile.ToJson());
        _snapshots = snapshots.ToDictionary(s => s.Schema.TableName, StringComparer.OrdinalIgnoreCase);
        _limits = limits;
        limits.Validate();
    }

    public static GenerationPlan Build(GenerationProfile profile, IReadOnlyList<GenerationTableSnapshot> snapshots,
        GenerationLimits? limits = null, CancellationToken ct = default, IProgress<GenerationProgress>? progress = null)
    {
        var plan = new GenerationPlan(profile, snapshots, limits ?? new());
        plan.Resolve(ct);
        plan.ValidateAllRows(ct, progress);
        return plan;
    }

    public static TableGenerationRule Suggest(GenerationTableSnapshot snapshot, IReadOnlySet<string>? selectedTables = null)
    {
        var schema = snapshot.Schema;
        var keyColumns = UniqueKeys(snapshot).SelectMany(k => k.Columns).ToHashSet(StringComparer.OrdinalIgnoreCase);
        return new TableGenerationRule
        {
            TableName = schema.TableName, SchemaId = schema.SchemaId,
            Columns = schema.Columns.Select(c => GeneratorCatalog.Suggest(c, keyColumns.Contains(c.Name))).ToList(),
            Relationships = schema.ForeignKeys.Select(fk => new RelationshipGenerationRule
            {
                Name = fk.ConstraintName, Columns = ChildColumns(fk).ToList(), ParentTable = fk.ReferencedTableName,
                ParentColumns = ParentColumns(fk).ToList(),
                Source = selectedTables?.Contains(fk.ReferencedTableName) == true && !Same(schema.TableName, fk.ReferencedTableName)
                    ? ParentKeySource.Generated : ParentKeySource.Existing,
            }).ToList(),
        };
    }

    public static IReadOnlyList<string> KeyColumns(GenerationTableSnapshot snapshot)
        => UniqueKeys(snapshot).SelectMany(k => k.Columns)
            .Concat(snapshot.Schema.ForeignKeys.SelectMany(ChildColumns))
            .Concat(snapshot.Schema.Columns.Where(c => c.IsIdentity).Select(c => c.Name))
            .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

    public TableSchema Schema(string table) => _tables[table].Snapshot.Schema;
    public IEnumerable<IReadOnlyDictionary<string, object?>> ExpectedFinalKeys(string table)
        => _snapshots[table].ExistingKeys.Concat(_tables.TryGetValue(table, out var planned) ? planned.GeneratedKeys : []);
    public static bool IsUserTable(string table) => !string.IsNullOrWhiteSpace(table)
        && !table.StartsWith("__", StringComparison.Ordinal) && !table.StartsWith("_etl_", StringComparison.OrdinalIgnoreCase)
        && !table.StartsWith("sys.", StringComparison.OrdinalIgnoreCase);
    public IEnumerable<IReadOnlyDictionary<string, object?>> Rows(string table, int offset = 0, int? count = null, CancellationToken ct = default)
    {
        var planned = _tables[table];
        int end = Math.Min(planned.Rule.Rows, checked(offset + (count ?? planned.Rule.Rows - offset)));
        if (offset < 0 || end < offset) throw new ArgumentOutOfRangeException(nameof(offset));
        for (int index = offset; index < end; index++)
        {
            ct.ThrowIfCancellationRequested();
            yield return Generate(planned, index + 1, ct);
        }
    }

    public IReadOnlyList<IReadOnlyDictionary<string, object?>> ReferencedKeys(string childTable, string relationship, int childOrdinal)
    {
        var child = _tables[childTable];
        var relation = child.Relations.First(r => r.Rule.Name == relationship);
        var row = Generate(child, childOrdinal, CancellationToken.None);
        if (relation.Rule.Columns.Any(c => row[c] is null)) return [];
        return relation.Pool.Where(parent => Matches(row, relation, parent)).Take(1).ToArray();
    }

    private void Resolve(CancellationToken ct)
    {
        if (_profile.Tables.Select(t => t.TableName).Distinct(StringComparer.OrdinalIgnoreCase).Count() != _profile.Tables.Count)
            throw new InvalidOperationException("A table may appear only once in a generation profile.");
        long total = 0;
        foreach (var rule in _profile.Tables)
        {
            ct.ThrowIfCancellationRequested();
            if (!IsUserTable(rule.TableName)) throw new InvalidOperationException("Internal and system tables cannot receive generated data.");
            if (rule.Rows < 0) throw new InvalidOperationException($"{rule.TableName}: row count cannot be negative.");
            total += rule.Rows;
            if (total > _limits.MaxRows) throw new InvalidOperationException($"This run exceeds the {_limits.MaxRows:N0} new-row limit.");
            if (!_snapshots.TryGetValue(rule.TableName, out var snapshot)) throw new InvalidOperationException($"Table '{rule.TableName}' no longer exists.");
            var schema = snapshot.Schema;
            if (rule.SchemaId != schema.SchemaId) throw new InvalidOperationException($"{rule.TableName}: the table identity changed. Recreate its configuration.");
            if (snapshot.InsertTriggers.Count > 0) throw new InvalidOperationException($"{schema.TableName}: INSERT triggers ({string.Join(", ", snapshot.InsertTriggers)}) require a separately qualified generation workflow.");
            if (rule.Columns is null || rule.Relationships is null || rule.Columns.Count != schema.Columns.Count
                || rule.Columns.Select(c => c.ColumnName).Distinct(StringComparer.OrdinalIgnoreCase).Count() != rule.Columns.Count)
                throw new InvalidOperationException($"{schema.TableName}: the column configuration does not match the schema.");
            var table = new PlannedTable(rule, snapshot);
            if (KeyColumns(snapshot).Any(name => schema.Columns[schema.GetColumnIndex(name)].IsRowVersion))
                throw new InvalidOperationException($"{schema.TableName}: generated rowversion keys are not supported.");
            foreach (var column in schema.Columns)
            {
                var field = rule.Columns.SingleOrDefault(c => Same(c.ColumnName, column.Name));
                if (field is null || field.SchemaId != column.SchemaId) throw new InvalidOperationException($"{schema.TableName}.{column.Name}: column identity changed.");
                GeneratorCatalog.Validate(field, column);
                long start = checked(snapshot.RowCount + 1);
                if (field.Generator == FieldGenerator.Sequence || column.IsIdentity)
                {
                    start = checked((long)decimal.Ceiling(field.Minimum));
                    foreach (var row in snapshot.ExistingKeys)
                        if (row.TryGetValue(column.Name, out object? existing) && existing is not null
                            && long.TryParse(GenerationValues.Display(existing), out long value)) start = Math.Max(start, checked(value + 1));
                    if (column.IsIdentity || column.IsPrimaryKey && column.Type == DbType.Integer) start = Math.Max(start, snapshot.Schema.NextRowId);
                }
                table.Starts[column.Name] = start;
            }
            table.Checks.AddRange(schema.CheckConstraints.Select(check => (check.ConstraintName ?? check.ExpressionSql, GenerationValues.ParseSafeExpression(check.ExpressionSql))));
            foreach (var fk in schema.ForeignKeys)
            {
                var configured = rule.Relationships.SingleOrDefault(r => Same(r.Name, fk.ConstraintName));
                if (configured is null || !Same(configured.ParentTable, fk.ReferencedTableName)
                    || !configured.Columns.SequenceEqual(ChildColumns(fk), StringComparer.OrdinalIgnoreCase)
                    || !configured.ParentColumns.SequenceEqual(ParentColumns(fk), StringComparer.OrdinalIgnoreCase))
                    throw new InvalidOperationException($"{schema.TableName}: declared relationship '{fk.ConstraintName}' must retain its schema mapping.");
            }
            if (rule.Relationships.Select(r => r.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count() != rule.Relationships.Count)
                throw new InvalidOperationException($"{schema.TableName}: relationship names must be unique.");
            _tables.Add(schema.TableName, table);
        }
        foreach (var table in _tables.Values)
        {
            foreach (var rule in table.Rule.Relationships)
            {
                ct.ThrowIfCancellationRequested();
                if (string.IsNullOrWhiteSpace(rule.Name) || rule.Columns is null || rule.ParentColumns is null || rule.Columns.Count == 0
                    || rule.Columns.Count != rule.ParentColumns.Count || rule.Columns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != rule.Columns.Count
                    || !_snapshots.TryGetValue(rule.ParentTable, out var parent))
                    throw new InvalidOperationException($"{table.Rule.TableName}: relationship '{rule.Name}' has an invalid mapping.");
                var childSchema = table.Snapshot.Schema;
                for (int i = 0; i < rule.Columns.Count; i++)
                {
                    int childIndex = childSchema.GetColumnIndex(rule.Columns[i]), parentIndex = parent.Schema.GetColumnIndex(rule.ParentColumns[i]);
                    if (childIndex < 0 || parentIndex < 0) throw new InvalidOperationException($"{rule.Name}: a mapped column no longer exists.");
                    if (childSchema.Columns[childIndex].Type != parent.Schema.Columns[parentIndex].Type
                        || childSchema.Columns[childIndex].IsRowVersion || parent.Schema.Columns[parentIndex].IsRowVersion)
                        throw new InvalidOperationException($"{rule.Name}: key columns must have compatible storage types and cannot be rowversion fields.");
                    if (rule.NullRate > 0 && !childSchema.Columns[childIndex].Nullable) throw new InvalidOperationException($"{rule.Name}: non-nullable key columns require zero null percentage.");
                }
                if (!UniqueKeys(parent).Any(k => k.Columns.SequenceEqual(rule.ParentColumns, StringComparer.OrdinalIgnoreCase)))
                    throw new InvalidOperationException($"{rule.Name}: parent columns must match an ordered primary or unique key.");
                if (!Enum.IsDefined(rule.Source) || rule.Distribution is not (FieldDistribution.Uniform or FieldDistribution.HotKeys)
                    || !double.IsFinite(rule.NullRate) || rule.NullRate is < 0 or > 1 || !double.IsFinite(rule.HotKeyRate) || rule.HotKeyRate is < 0 or > 1)
                    throw new InvalidOperationException($"{rule.Name}: check parent source, distribution, and percentages.");
                if (rule.Source == ParentKeySource.Generated && !_tables.ContainsKey(rule.ParentTable))
                    throw new InvalidOperationException($"{rule.Name}: select '{rule.ParentTable}' or use existing parent keys.");
                bool unique = UniqueKeys(table.Snapshot).Any(k => k.Columns.All(c => rule.Columns.Contains(c, StringComparer.OrdinalIgnoreCase)));
                if (unique && rule.Distribution == FieldDistribution.HotKeys) throw new InvalidOperationException($"{rule.Name}: a unique child key requires selection without replacement.");
                table.Relations.Add(new ResolvedRelationship(rule, parent, unique));
            }
        }
        var order = new List<string>();
        var visiting = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        void Visit(string name)
        {
            if (visited.Contains(name)) return;
            if (!visiting.Add(name)) throw new InvalidOperationException($"Generated-key cycle at '{name}'. Use existing parent keys or a separate workflow.");
            foreach (var relation in _tables[name].Relations.Where(r => r.Rule.Source == ParentKeySource.Generated)) Visit(relation.Rule.ParentTable);
            visiting.Remove(name); visited.Add(name); order.Add(name);
        }
        foreach (string table in _tables.Keys.Order(StringComparer.OrdinalIgnoreCase)) Visit(table);
        Order = order.AsReadOnly();
        foreach (var snapshot in _snapshots.Values)
            foreach (var key in snapshot.ExistingKeys) AddKeyBytes(GenerationValues.Size(key.Values));
    }

    private void ValidateAllRows(CancellationToken ct, IProgress<GenerationProgress>? progress)
    {
        long processed = 0, total = _tables.Values.Sum(t => (long)t.Rule.Rows);
        foreach (string name in Order)
        {
            var table = _tables[name];
            foreach (var relation in table.Relations)
            {
                var source = relation.Rule.Source == ParentKeySource.Generated ? _tables[relation.Rule.ParentTable].GeneratedKeys : relation.Parent.ExistingKeys;
                var occupied = new HashSet<string>(StringComparer.Ordinal);
                if (relation.Unique)
                    foreach (var existing in table.Snapshot.ExistingKeys.Where(row => relation.Rule.Columns.All(c => row.GetValueOrDefault(c) is not null)))
                    {
                        var mapped = relation.Rule.ParentColumns.Select((column, index) => (column, value: existing[relation.Rule.Columns[index]]))
                            .ToDictionary(p => p.column, p => p.value, StringComparer.OrdinalIgnoreCase);
                        string key = GenerationValues.Key(relation.Parent.Schema, relation.Rule.ParentColumns, mapped);
                        if (occupied.Add(key)) AddKeyBytes(key.Length * 2L + 48);
                    }
                relation.Pool = source.Where(row => relation.Rule.ParentColumns.All(c => row.GetValueOrDefault(c) is not null))
                    .Where(row => !occupied.Contains(GenerationValues.Key(relation.Parent.Schema, relation.Rule.ParentColumns, row)))
                    .OrderBy(row => GenerationValues.Key(relation.Parent.Schema, relation.Rule.ParentColumns, row), StringComparer.Ordinal).ToArray();
                foreach (var parent in relation.Pool)
                {
                    string key = GenerationValues.Key(relation.Parent.Schema, relation.Rule.ParentColumns, parent);
                    if (relation.Keys.Add(key)) AddKeyBytes(key.Length * 2L + 48);
                }
                if (relation.Pool.Count == 0 && table.Rule.Rows > 0 && relation.Rule.NullRate < 1)
                    throw new InvalidOperationException($"{name}.{relation.Rule.Name}: the parent key pool is empty.");
                if (relation.Unique && table.Rule.Rows > relation.Pool.Count && relation.Rule.NullRate == 0)
                    throw new InvalidOperationException($"{name}.{relation.Rule.Name}: {table.Rule.Rows} unique children require at least that many parent keys.");
            }
            var keys = UniqueKeys(table.Snapshot).ToArray();
            var seen = keys.Select(_ => new HashSet<string>(StringComparer.Ordinal)).ToArray();
            foreach (var existing in table.Snapshot.ExistingKeys)
                for (int k = 0; k < keys.Length; k++)
                    if (keys[k].Columns.All(c => existing.GetValueOrDefault(c) is not null))
                    {
                        string key = GenerationValues.Key(table.Snapshot.Schema, keys[k].Columns, existing, keys[k].Collations);
                        if (seen[k].Add(key)) AddKeyBytes(key.Length * 2L + 48);
                    }
            string[] retained = KeyColumns(table.Snapshot).Concat(table.Rule.Relationships.SelectMany(r => r.Columns))
                .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            string[] insertedColumns = table.Snapshot.Schema.Columns.Where(c => !c.IsRowVersion).Select(c => c.Name).ToArray();
            int statementOverhead = System.Text.Encoding.UTF8.GetByteCount(GenerationValues.InsertPrefix(name, insertedColumns)) + 1;
            for (int ordinal = 1; ordinal <= table.Rule.Rows; ordinal++)
            {
                ct.ThrowIfCancellationRequested();
                var row = Generate(table, ordinal, ct);
                foreach (var relation in table.Relations)
                {
                    if (relation.Rule.Columns.Any(c => row[c] is null)) continue;
                    var mapped = relation.Rule.ParentColumns.Select((column, index) => (column, value: row[relation.Rule.Columns[index]]))
                        .ToDictionary(p => p.column, p => p.value, StringComparer.OrdinalIgnoreCase);
                    if (!relation.Keys.Contains(GenerationValues.Key(relation.Parent.Schema, relation.Rule.ParentColumns, mapped)))
                        throw new InvalidOperationException($"{name} row {ordinal}: assignment to the child column types changes a key in '{relation.Rule.Name}'.");
                }
                if (statementOverhead + System.Text.Encoding.UTF8.GetByteCount(GenerationValues.RowLiteral(insertedColumns, row)) > _limits.MaxStatementBytes)
                    throw new InvalidOperationException($"{name} row {ordinal} exceeds the insert statement byte limit.");
                GeneratedBytes += GenerationValues.Size(row.Values);
                if (GeneratedBytes > _limits.MaxGeneratedBytes) throw new InvalidOperationException("Generated data exceeds the configured byte limit.");
                foreach (var check in table.Checks)
                {
                    DbValue valid = GenerationValues.Evaluate(check.Expression, table.Snapshot.Schema, row);
                    if (!valid.IsNull && !valid.IsTruthy) throw new InvalidOperationException($"{name} row {ordinal} fails CHECK '{check.Name}'. Adjust the field rules.");
                }
                for (int k = 0; k < keys.Length; k++)
                {
                    if (keys[k].Columns.Any(c => row.GetValueOrDefault(c) is null)) continue;
                    string key = GenerationValues.Key(table.Snapshot.Schema, keys[k].Columns, row, keys[k].Collations);
                    if (!seen[k].Add(key)) throw new InvalidOperationException($"{name} row {ordinal} duplicates key ({string.Join(", ", keys[k].Columns)}). Use a sequence, a larger domain, or different parent keys.");
                    AddKeyBytes(key.Length * 2L + 48);
                }
                var retainedRow = retained.ToDictionary(c => c, c => row[c], StringComparer.OrdinalIgnoreCase);
                AddKeyBytes(GenerationValues.Size(retainedRow.Values));
                table.GeneratedKeys.Add(retainedRow);
                if (++processed % 100 == 0 || processed == total) progress?.Report(new(name, processed, total, "Validating"));
            }
        }
    }

    private IReadOnlyDictionary<string, object?> Generate(PlannedTable table, int ordinal, CancellationToken ct)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var relation in table.Relations.OrderBy(r => r.Rule.Name, StringComparer.Ordinal))
        {
            ct.ThrowIfCancellationRequested();
            var random = StableRandom.Create(_profile.Seed, table.Snapshot.Schema.SchemaId, table.Rule.TableName, relation.Rule.Name, ordinal);
            bool useNull = relation.Rule.NullRate > 0 && random.NextDouble() < relation.Rule.NullRate;
            if (useNull)
            {
                foreach (string column in relation.Rule.Columns)
                {
                    if (values.TryGetValue(column, out object? previous) && previous is not null) throw new InvalidOperationException($"{relation.Rule.Name}: overlapping relationship null policies conflict.");
                    values[column] = null;
                }
                continue;
            }
            var candidates = relation.Pool;
            string[] overlap = relation.Rule.Columns.Where(values.ContainsKey).ToArray();
            if (overlap.Length > 0)
            {
                string cacheKey = System.Text.Json.JsonSerializer.Serialize(overlap.Select(c => GenerationValues.Display(values[c])));
                if (!relation.OverlapPools.TryGetValue(cacheKey, out candidates))
                {
                    candidates = relation.Pool.Where(parent =>
                    {
                        ct.ThrowIfCancellationRequested();
                        return Matches(values, relation, parent, overlap);
                    }).ToArray();
                    relation.OverlapPools.Add(cacheKey, candidates);
                    AddKeyBytes(candidates.Count * 8L + cacheKey.Length * 2L + 48);
                }
            }
            if (candidates.Count == 0) throw new InvalidOperationException($"{table.Rule.TableName}.{relation.Rule.Name}: no parent keys satisfy the mapped columns.");
            int index = relation.Unique ? ordinal - 1
                : relation.Rule.Distribution == FieldDistribution.HotKeys && random.NextDouble() < relation.Rule.HotKeyRate
                    ? random.Next(Math.Max(1, candidates.Count / 5)) : random.Next(candidates.Count);
            if (index >= candidates.Count) throw new InvalidOperationException($"{relation.Rule.Name}: insufficient parent keys for unique selection.");
            var chosen = candidates[index];
            for (int i = 0; i < relation.Rule.Columns.Count; i++) values[relation.Rule.Columns[i]] = chosen[relation.Rule.ParentColumns[i]];
        }
        var schema = table.Snapshot.Schema;
        foreach (var rule in table.Rule.Columns.OrderBy(r => r.Generator is FieldGenerator.Email or FieldGenerator.FullName ? 1 : r.Generator == FieldGenerator.Default ? 2 : 0))
        {
            var column = schema.Columns[schema.GetColumnIndex(rule.ColumnName)];
            if (column.IsRowVersion) continue;
            object? value = values.TryGetValue(column.Name, out object? reference) ? reference
                : GeneratorCatalog.Generate(_profile, schema, column, rule, ordinal, table.Starts[column.Name], values);
            values[column.Name] = GenerationValues.ToObject(GenerationValues.Assign(value, column, schema.TableName));
        }
        return values;
    }

    private static bool Matches(IReadOnlyDictionary<string, object?> child, ResolvedRelationship relation,
        IReadOnlyDictionary<string, object?> parent, IReadOnlyList<string>? subset = null)
    {
        var mapped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < relation.Rule.Columns.Count; i++)
            if (subset is null || subset.Contains(relation.Rule.Columns[i], StringComparer.OrdinalIgnoreCase))
                mapped[relation.Rule.ParentColumns[i]] = child[relation.Rule.Columns[i]];
        string[] columns = mapped.Keys.ToArray();
        return GenerationValues.Key(relation.Parent.Schema, columns, mapped) == GenerationValues.Key(relation.Parent.Schema, columns, parent);
    }

    private void AddKeyBytes(long bytes)
    {
        _keyBytes = checked(_keyBytes + bytes);
        if (_keyBytes > _limits.MaxKeyBytes) throw new InvalidOperationException("Key validation exceeds the configured memory limit. Reduce the run or use a smaller parent table.");
    }
    private static bool Same(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    private static IReadOnlyList<string> ChildColumns(ForeignKeyDefinition fk) => fk.ColumnNames.Count > 0 ? fk.ColumnNames : [fk.ColumnName];
    private static IReadOnlyList<string> ParentColumns(ForeignKeyDefinition fk) => fk.ReferencedColumnNames.Count > 0 ? fk.ReferencedColumnNames : [fk.ReferencedColumnName];
    private static IEnumerable<UniqueKey> UniqueKeys(GenerationTableSnapshot snapshot)
    {
        var schema = snapshot.Schema;
        foreach (var key in schema.KeyConstraints) yield return new(key.Columns, null);
        if (!schema.KeyConstraints.Any(k => k.Kind == KeyConstraintKind.PrimaryKey))
        {
            string[] primary = schema.Columns.Where(c => c.IsPrimaryKey).Select(c => c.Name).ToArray();
            if (primary.Length > 0) yield return new(primary, null);
        }
        foreach (var index in snapshot.Indexes.Where(i => i.IsUnique)) yield return new(index.Columns, index.ColumnCollations);
    }
    private sealed record UniqueKey(IReadOnlyList<string> Columns, IReadOnlyList<string?>? Collations);
    private sealed class PlannedTable(TableGenerationRule rule, GenerationTableSnapshot snapshot)
    {
        public TableGenerationRule Rule { get; } = rule;
        public GenerationTableSnapshot Snapshot { get; } = snapshot;
        public Dictionary<string, long> Starts { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<ResolvedRelationship> Relations { get; } = [];
        public List<(string Name, Expression Expression)> Checks { get; } = [];
        public List<IReadOnlyDictionary<string, object?>> GeneratedKeys { get; } = [];
    }
    private sealed class ResolvedRelationship(RelationshipGenerationRule rule, GenerationTableSnapshot parent, bool unique)
    {
        public RelationshipGenerationRule Rule { get; } = rule;
        public GenerationTableSnapshot Parent { get; } = parent;
        public bool Unique { get; } = unique;
        public IReadOnlyList<IReadOnlyDictionary<string, object?>> Pool { get; set; } = [];
        public HashSet<string> Keys { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, IReadOnlyList<IReadOnlyDictionary<string, object?>>> OverlapPools { get; } = new(StringComparer.Ordinal);
    }
}
