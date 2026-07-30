using System.Buffers.Binary;
using System.Security.Cryptography;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;

namespace CSharpDB.Tests;

public sealed class ReleasedDatabaseUpgradeTests
{
    [Theory]
    [InlineData(
        "csharpdb-v4.0.0.db",
        "v4.0.0",
        1,
        "f96465d2541c744f533859dddd6ce6003475b71ca6a08466fc6efaded3ae5a35")]
    [InlineData(
        "csharpdb-v4.0.1.db",
        "v4.0.1",
        1,
        "4e8ba1b47a0cdc129f84a476ad261432948f9f86932bbe889477ecec6dada3d0")]
    [InlineData(
        "csharpdb-v4.0.2.db",
        "v4.0.2",
        1,
        "fb2bea18690a58ed6e027bc7fc013d810cf644c2d633330bf005b384ce527315")]
    [InlineData(
        "csharpdb-v4.0.3.db",
        "v4.0.3",
        2,
        "0cfe60af58d778b72f83429513bf43dbfe3e164511066a18f067dece8533d953")]
    [InlineData(
        "csharpdb-v4.0.4.db",
        "v4.0.4",
        2,
        "dab0c94039d049edcb76a7c2615e40c67710da7c27e3e50e858143618da7c0ee")]
    [InlineData(
        "csharpdb-v4.1.0.db",
        "v4.1.0",
        2,
        "4cb3d49d410e38b65ecbf8d45e5e5e6ff361e7ee44a983119a15e39ee6421530")]
    [InlineData(
        "csharpdb-v4.2.0.db",
        "v4.2.0",
        2,
        "989b70ddd00a0e11a399e156f29309069b3c6b197e72752b9b8942289add8d7c")]
    [InlineData(
        "csharpdb-v4.3.0.db",
        "v4.3.0",
        2,
        "fc642aecd3cc0d909bf5a71ef828b2de9f2de47d6321bdffd3767b408747bb20")]
    public async Task ReleasedDatabase_UpgradesThroughCurrentWriteCheckpointAndReopen(
        string fixtureName,
        string releaseTag,
        int releasedFormatVersion,
        string expectedSha256)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string fixturePath = Path.Combine(
            AppContext.BaseDirectory,
            "Fixtures",
            "ReleasedDatabases",
            fixtureName);
        string workspace = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_released_upgrade_{Guid.NewGuid():N}");
        string databasePath = Path.Combine(workspace, fixtureName);

        Assert.True(File.Exists(fixturePath), $"Released database fixture not found: {fixturePath}");
        Assert.Equal(expectedSha256, await ComputeSha256Async(fixturePath, ct));
        Assert.Equal(releasedFormatVersion, await ReadFormatVersionAsync(fixturePath, ct));

        Directory.CreateDirectory(workspace);
        File.Copy(fixturePath, databasePath, overwrite: false);

        try
        {
            await using (Database database = await Database.OpenAsync(databasePath, ct))
            {
                Assert.Equal(
                    releaseTag,
                    await ScalarTextAsync(
                        database,
                        "SELECT release_tag FROM fixture_origin WHERE id = 1",
                        ct));
                Assert.Equal(
                    2L,
                    await ScalarIntegerAsync(database, "SELECT COUNT(*) FROM accounts", ct));
                Assert.Contains(
                    database.GetIndexes(),
                    index => string.Equals(
                        index.IndexName,
                        "ix_accounts_name",
                        StringComparison.OrdinalIgnoreCase));

                await database.ExecuteAsync(
                    "ALTER TABLE accounts " +
                    "ADD COLUMN status TEXT NOT NULL DEFAULT 'legacy'",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO accounts (id, name, score, payload, status) " +
                    "VALUES (3, 'Gamma', 4.25, X'FF', 'current')",
                    ct);
                await database.ExecuteAsync(
                    """
                    CREATE TABLE upgrade_parents (
                        tenant_id INTEGER,
                        code TEXT,
                        CONSTRAINT pk_upgrade_parents
                            PRIMARY KEY (tenant_id, code)
                    )
                    """,
                    ct);
                await database.ExecuteAsync(
                    """
                    CREATE TABLE upgrade_children (
                        id INTEGER PRIMARY KEY,
                        tenant_id INTEGER,
                        parent_code TEXT,
                        CONSTRAINT fk_upgrade_children_parent
                            FOREIGN KEY (tenant_id, parent_code)
                            REFERENCES upgrade_parents (tenant_id, code)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
                    )
                    """,
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO upgrade_parents VALUES (9, 'alpha')",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO upgrade_children VALUES (90, 9, 'alpha')",
                    ct);
                await database.ExecuteAsync(
                    "UPDATE upgrade_parents SET code = 'beta' " +
                    "WHERE tenant_id = 9 AND code = 'alpha'",
                    ct);

                Assert.Equal(
                    "beta",
                    await ScalarTextAsync(
                        database,
                        "SELECT parent_code FROM upgrade_children WHERE id = 90",
                        ct));
                await database.CheckpointAsync(ct);
            }

            Assert.Equal(
                PageConstants.FormatVersion,
                await ReadFormatVersionAsync(databasePath, ct));

            await using (Database reopened = await Database.OpenAsync(databasePath, ct))
            {
                Assert.Equal(
                    releaseTag,
                    await ScalarTextAsync(
                        reopened,
                        "SELECT release_tag FROM fixture_origin WHERE id = 1",
                        ct));

                await using (QueryResult accounts = await reopened.ExecuteAsync(
                    "SELECT id, name, score, payload, status FROM accounts ORDER BY id",
                    ct))
                {
                    List<DbValue[]> rows = await accounts.ToListAsync(ct);
                    Assert.Equal(3, rows.Count);

                    Assert.Equal(1L, rows[0][0].AsInteger);
                    Assert.Equal("Alpha", rows[0][1].AsText);
                    Assert.Equal(1.5D, rows[0][2].AsReal);
                    Assert.Equal(new byte[] { 0x01, 0x02 }, rows[0][3].AsBlob);
                    Assert.Equal("legacy", rows[0][4].AsText);

                    Assert.Equal(2L, rows[1][0].AsInteger);
                    Assert.True(rows[1][1].IsNull);
                    Assert.Equal(-2.25D, rows[1][2].AsReal);
                    Assert.Empty(rows[1][3].AsBlob);
                    Assert.Equal("legacy", rows[1][4].AsText);

                    Assert.Equal(3L, rows[2][0].AsInteger);
                    Assert.Equal("Gamma", rows[2][1].AsText);
                    Assert.Equal(4.25D, rows[2][2].AsReal);
                    Assert.Equal(new byte[] { 0xFF }, rows[2][3].AsBlob);
                    Assert.Equal("current", rows[2][4].AsText);
                }

                TableSchema accountsSchema = Assert.IsType<TableSchema>(
                    reopened.GetTableSchema("accounts"));
                ColumnDefinition status = Assert.Single(
                    accountsSchema.Columns,
                    column => string.Equals(
                        column.Name,
                        "status",
                        StringComparison.OrdinalIgnoreCase));
                Assert.Equal("'legacy'", status.DefaultSql);
                Assert.False(status.Nullable);
                Assert.NotEqual(Guid.Empty, accountsSchema.SchemaId);
                Assert.All(
                    accountsSchema.Columns,
                    column => Assert.NotEqual(Guid.Empty, column.SchemaId));

                TableSchema childSchema = Assert.IsType<TableSchema>(
                    reopened.GetTableSchema("upgrade_children"));
                ForeignKeyDefinition foreignKey = Assert.Single(childSchema.ForeignKeys);
                Assert.Equal("fk_upgrade_children_parent", foreignKey.ConstraintName);
                Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
                Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnUpdate);
                Assert.Equal(["tenant_id", "parent_code"], foreignKey.ColumnNames);
                Assert.Equal(["tenant_id", "code"], foreignKey.ReferencedColumnNames);

                await reopened.ExecuteAsync(
                    "DELETE FROM upgrade_parents " +
                    "WHERE tenant_id = 9 AND code = 'beta'",
                    ct);
                Assert.Equal(
                    0L,
                    await ScalarIntegerAsync(
                        reopened,
                        "SELECT COUNT(*) FROM upgrade_children",
                        ct));

                await reopened.ExecuteAsync(
                    "INSERT INTO accounts (id, name, score, payload) " +
                    "VALUES (4, 'Delta', 8.5, X'0A0B')",
                    ct);
                Assert.Equal(
                    "legacy",
                    await ScalarTextAsync(
                        reopened,
                        "SELECT status FROM accounts WHERE id = 4",
                        ct));
                await reopened.CheckpointAsync(ct);
            }

            Assert.Equal(
                PageConstants.FormatVersion,
                await ReadFormatVersionAsync(databasePath, ct));
            Assert.Equal(expectedSha256, await ComputeSha256Async(fixturePath, ct));
        }
        finally
        {
            if (Directory.Exists(workspace))
                Directory.Delete(workspace, recursive: true);
        }
    }

    private static async Task<long> ScalarIntegerAsync(
        Database database,
        string sql,
        CancellationToken ct)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, ct);
        return Assert.Single(await result.ToListAsync(ct))[0].AsInteger;
    }

    private static async Task<string> ScalarTextAsync(
        Database database,
        string sql,
        CancellationToken ct)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, ct);
        return Assert.Single(await result.ToListAsync(ct))[0].AsText;
    }

    private static async Task<int> ReadFormatVersionAsync(
        string path,
        CancellationToken ct)
    {
        byte[] header = new byte[PageConstants.VersionOffset + sizeof(int)];
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read);
        await stream.ReadExactlyAsync(header, ct);
        return BinaryPrimitives.ReadInt32LittleEndian(
            header.AsSpan(PageConstants.VersionOffset, sizeof(int)));
    }

    private static async Task<string> ComputeSha256Async(
        string path,
        CancellationToken ct)
    {
        byte[] contents = await File.ReadAllBytesAsync(path, ct);
        return Convert.ToHexString(SHA256.HashData(contents)).ToLowerInvariant();
    }
}
