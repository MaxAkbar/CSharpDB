using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Tests;

public sealed class NumericRelationshipIndexJoinOperatorTests
{
    [Fact]
    public async Task FullProjection_MatchesIndexNestedLoop_ForFanoutOrphanAndNullKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fixture = await RelationshipFixture.CreateAsync(ct);

        DbValue[][] candidateRows;
        await using (var candidate = fixture.CreateCandidate(IndexScanRange.All))
        {
            candidateRows = OrderRows(
                await ReadRowsAsync(candidate, ct),
                leftKeyIndex: 0,
                rightKeyIndex: fixture.LeftSchema.Columns.Count);
        }

        DbValue[][] baselineRows;
        await using (var baseline = fixture.CreateIndexNestedLoop())
        {
            baselineRows = OrderRows(
                await ReadRowsAsync(baseline, ct),
                leftKeyIndex: 0,
                rightKeyIndex: fixture.LeftSchema.Columns.Count);
        }

        AssertRowsEqual(baselineRows, candidateRows);
        Assert.Equal([10L, 11L, 12L], candidateRows.Select(row => row[3].AsInteger).ToArray());
        Assert.Equal(["P1", "P1", "P2"], candidateRows.Select(row => row[1].AsText).ToArray());
        Assert.Equal([100L, 200L, 50L], candidateRows.Select(row => row[5].AsInteger).ToArray());
        Assert.Equal(2, candidateRows.Count(row => row[0].AsInteger == 1));
        Assert.DoesNotContain(candidateRows, row => row[0].AsInteger == 3);
        Assert.DoesNotContain(candidateRows, row => row[3].AsInteger is 13 or 14);
    }

    [Fact]
    public async Task KeyOnlyProjection_MatchesIndexNestedLoop_WithoutBaseColumnProjection()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fixture = await RelationshipFixture.CreateAsync(ct);
        int[] projection = fixture.CreateKeyOnlyProjectionIndices();
        ColumnDefinition[] outputSchema = fixture.CreateKeyOnlyOutputSchema();

        DbValue[][] candidateRows;
        await using (var candidate = fixture.CreateCandidate(IndexScanRange.All))
        {
            Assert.True(candidate.TrySetOutputProjection(projection, outputSchema));
            candidateRows = OrderRows(
                await ReadBatchRowsAsync(candidate, ct),
                leftKeyIndex: 0,
                rightKeyIndex: 1);
            Assert.Null(GetPrivateField<DbValue[]>(candidate, "_leftRowBuffer"));
            Assert.Null(GetPrivateField<DbValue[]>(candidate, "_rightRowBuffer"));
        }

        DbValue[][] baselineRows;
        await using (var baseline = fixture.CreateIndexNestedLoop())
        {
            Assert.True(baseline.TrySetOutputProjection(projection, outputSchema));
            baselineRows = OrderRows(
                await ReadBatchRowsAsync(baseline, ct),
                leftKeyIndex: 0,
                rightKeyIndex: 1);
        }

        AssertRowsEqual(baselineRows, candidateRows);
        Assert.Equal(
            [(1L, 10L, 1L), (1L, 11L, 1L), (2L, 12L, 2L)],
            candidateRows
                .Select(row => (row[0].AsInteger, row[1].AsInteger, row[2].AsInteger))
                .ToArray());
    }

    [Theory]
    [InlineData(0L, 0)]
    [InlineData(1L, 2)]
    [InlineData(2L, 1)]
    [InlineData(3L, 0)]
    [InlineData(5L, 0)]
    [InlineData(10L, 0)]
    [InlineData(99L, 0)]
    public async Task PointRange_MatchesIndexNestedLoop(long joinKey, int expectedRowCount)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fixture = await RelationshipFixture.CreateAsync(ct);
        int[] projection = fixture.CreateKeyOnlyProjectionIndices();
        ColumnDefinition[] outputSchema = fixture.CreateKeyOnlyOutputSchema();

        DbValue[][] candidateRows;
        await using (var candidate = fixture.CreateCandidate(IndexScanRange.At(joinKey)))
        {
            Assert.True(candidate.TrySetOutputProjection(projection, outputSchema));
            candidateRows = OrderRows(
                await ReadRowsAsync(candidate, ct),
                leftKeyIndex: 0,
                rightKeyIndex: 1);
        }

        DbValue[][] baselineRows;
        await using (var baseline = fixture.CreateIndexNestedLoop(joinKey))
        {
            Assert.True(baseline.TrySetOutputProjection(projection, outputSchema));
            baselineRows = OrderRows(
                await ReadRowsAsync(baseline, ct),
                leftKeyIndex: 0,
                rightKeyIndex: 1);
        }

        AssertRowsEqual(baselineRows, candidateRows);
        Assert.Equal(expectedRowCount, candidateRows.Length);
        Assert.All(candidateRows, row =>
        {
            Assert.Equal(joinKey, row[0].AsInteger);
            Assert.Equal(joinKey, row[2].AsInteger);
        });
    }

    [Fact]
    public async Task RepeatedOpen_RowAndBatchEnumerationRemainEquivalent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fixture = await RelationshipFixture.CreateAsync(ct);
        int[] projection = fixture.CreateKeyOnlyProjectionIndices();
        ColumnDefinition[] outputSchema = fixture.CreateKeyOnlyOutputSchema();

        await using var candidate = fixture.CreateCandidate(IndexScanRange.All);
        Assert.True(candidate.TrySetOutputProjection(projection, outputSchema));

        DbValue[][] firstRowPass = OrderRows(
            await ReadRowsAsync(candidate, ct),
            leftKeyIndex: 0,
            rightKeyIndex: 1);
        DbValue[][] secondRowPass = OrderRows(
            await ReadRowsAsync(candidate, ct),
            leftKeyIndex: 0,
            rightKeyIndex: 1);
        DbValue[][] batchPass = OrderRows(
            await ReadBatchRowsAsync(candidate, ct),
            leftKeyIndex: 0,
            rightKeyIndex: 1);

        AssertRowsEqual(firstRowPass, secondRowPass);
        AssertRowsEqual(firstRowPass, batchPass);

        await candidate.DisposeAsync();
        DbValue[][] reopenedAfterDispose = OrderRows(
            await ReadRowsAsync(candidate, ct),
            leftKeyIndex: 0,
            rightKeyIndex: 1);
        AssertRowsEqual(firstRowPass, reopenedAfterDispose);
    }

    private static async ValueTask<DbValue[][]> ReadRowsAsync(IOperator op, CancellationToken ct)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(ct);
        while (await op.MoveNextAsync(ct))
            rows.Add((DbValue[])op.Current.Clone());
        return rows.ToArray();
    }

    private static async ValueTask<DbValue[][]> ReadBatchRowsAsync(IBatchOperator op, CancellationToken ct)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(ct);
        while (await op.MoveNextBatchAsync(ct))
        {
            RowBatch batch = op.CurrentBatch;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                rows.Add(batch.GetRowSpan(rowIndex).ToArray());
        }

        return rows.ToArray();
    }

    private static DbValue[][] OrderRows(
        IEnumerable<DbValue[]> rows,
        int leftKeyIndex,
        int rightKeyIndex)
        => rows
            .OrderBy(row => row[leftKeyIndex].AsInteger)
            .ThenBy(row => row[rightKeyIndex].AsInteger)
            .ToArray();

    private static void AssertRowsEqual(IReadOnlyList<DbValue[]> expected, IReadOnlyList<DbValue[]> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int rowIndex = 0; rowIndex < expected.Count; rowIndex++)
        {
            Assert.Equal(expected[rowIndex].Length, actual[rowIndex].Length);
            for (int columnIndex = 0; columnIndex < expected[rowIndex].Length; columnIndex++)
                Assert.Equal(expected[rowIndex][columnIndex], actual[rowIndex][columnIndex]);
        }
    }

    private static T? GetPrivateField<T>(object target, string fieldName)
    {
        FieldInfo field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' not found on {target.GetType().Name}.");
        return (T?)field.GetValue(target);
    }

    private sealed class RelationshipFixture : IAsyncDisposable
    {
        private const string LeftTableName = "rel_parent";
        private const string RightTableName = "rel_child";
        private const string ForeignKeyIndexName = "idx_rel_child_parent_id";

        private readonly Database _db;
        private readonly BTree _leftTree;
        private readonly BTree _rightTree;
        private readonly IIndexStore _foreignKeyIndex;
        private readonly int _rightForeignKeyColumnIndex;

        private RelationshipFixture(
            Database db,
            BTree leftTree,
            BTree rightTree,
            IIndexStore foreignKeyIndex,
            TableSchema leftSchema,
            TableSchema rightSchema,
            int rightForeignKeyColumnIndex)
        {
            _db = db;
            _leftTree = leftTree;
            _rightTree = rightTree;
            _foreignKeyIndex = foreignKeyIndex;
            LeftSchema = leftSchema;
            RightSchema = rightSchema;
            _rightForeignKeyColumnIndex = rightForeignKeyColumnIndex;
        }

        public TableSchema LeftSchema { get; }

        public TableSchema RightSchema { get; }

        public static async ValueTask<RelationshipFixture> CreateAsync(CancellationToken ct)
        {
            Database db = await Database.OpenInMemoryAsync(ct);
            try
            {
                await db.ExecuteAsync(
                    $"CREATE TABLE {LeftTableName} (id INTEGER PRIMARY KEY, label TEXT, active INTEGER NOT NULL)",
                    ct);
                await db.ExecuteAsync(
                    $"CREATE TABLE {RightTableName} (id INTEGER PRIMARY KEY, parent_id INTEGER, amount INTEGER NOT NULL)",
                    ct);
                await db.ExecuteAsync(
                    $"CREATE INDEX {ForeignKeyIndexName} ON {RightTableName}(parent_id)",
                    ct);

                await db.ExecuteAsync($"INSERT INTO {LeftTableName} VALUES (1, 'P1', 1)", ct);
                await db.ExecuteAsync($"INSERT INTO {LeftTableName} VALUES (2, 'P2', 0)", ct);
                await db.ExecuteAsync($"INSERT INTO {LeftTableName} VALUES (3, 'P3', 1)", ct);
                await db.ExecuteAsync($"INSERT INTO {LeftTableName} VALUES (10, 'P10', 1)", ct);

                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (10, 1, 100)", ct);
                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (11, 1, 200)", ct);
                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (12, 2, 50)", ct);
                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (13, 99, 900)", ct);
                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (14, NULL, 700)", ct);
                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (15, 0, 800)", ct);
                await db.ExecuteAsync($"INSERT INTO {RightTableName} VALUES (16, 5, 850)", ct);

                SchemaCatalog catalog = GetCatalog(db);
                TableSchema leftSchema = catalog.GetTable(LeftTableName)
                    ?? throw new InvalidOperationException($"Table '{LeftTableName}' was not found.");
                TableSchema rightSchema = catalog.GetTable(RightTableName)
                    ?? throw new InvalidOperationException($"Table '{RightTableName}' was not found.");
                int rightForeignKeyColumnIndex = rightSchema.GetColumnIndex("parent_id");
                Assert.True(rightForeignKeyColumnIndex >= 0);

                return new RelationshipFixture(
                    db,
                    catalog.GetTableTree(LeftTableName),
                    catalog.GetTableTree(RightTableName),
                    catalog.GetIndexStore(ForeignKeyIndexName),
                    leftSchema,
                    rightSchema,
                    rightForeignKeyColumnIndex);
            }
            catch
            {
                await db.DisposeAsync();
                throw;
            }
        }

        public NumericRelationshipIndexJoinOperator CreateCandidate(IndexScanRange range)
            => new(
                _leftTree,
                _rightTree,
                _foreignKeyIndex,
                LeftSchema,
                RightSchema,
                _rightForeignKeyColumnIndex,
                range);

        public IndexNestedLoopJoinOperator CreateIndexNestedLoop(long? pointKey = null)
        {
            IOperator outer = pointKey.HasValue
                ? new PrimaryKeyLookupOperator(_leftTree, LeftSchema, pointKey.Value)
                : new TableScanOperator(_leftTree, LeftSchema);

            return new IndexNestedLoopJoinOperator(
                outer,
                _rightTree,
                _foreignKeyIndex,
                JoinType.Inner,
                LeftSchema.PrimaryKeyColumnIndex,
                LeftSchema.Columns.Count,
                RightSchema.Columns.Count,
                residualCondition: null,
                TableSchema.CreateJoinSchema(LeftSchema, RightSchema));
        }

        public int[] CreateKeyOnlyProjectionIndices()
            =>
            [
                LeftSchema.PrimaryKeyColumnIndex,
                LeftSchema.Columns.Count + RightSchema.PrimaryKeyColumnIndex,
                LeftSchema.Columns.Count + _rightForeignKeyColumnIndex,
            ];

        public ColumnDefinition[] CreateKeyOnlyOutputSchema()
            =>
            [
                LeftSchema.Columns[LeftSchema.PrimaryKeyColumnIndex],
                RightSchema.Columns[RightSchema.PrimaryKeyColumnIndex],
                RightSchema.Columns[_rightForeignKeyColumnIndex],
            ];

        public ValueTask DisposeAsync() => _db.DisposeAsync();

        private static SchemaCatalog GetCatalog(Database db)
        {
            FieldInfo field = typeof(Database).GetField("_catalog", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("Database catalog field not found.");
            return (SchemaCatalog?)field.GetValue(db)
                ?? throw new InvalidOperationException("Database catalog was not initialized.");
        }
    }
}
