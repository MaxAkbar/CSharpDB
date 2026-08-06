using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class BitStringDataTypeTests
{
    [Fact]
    public void PhysicalRoundTrip_PreservesBitLengthAndDistinguishesRawBlobs()
    {
        DbValue oneBit = DbValue.FromBitString([0x80], 1);
        DbValue eightBits = DbValue.FromBitString([0x80], 8);
        DbValue rawBlob = DbValue.FromBlob([0x80]);

        Assert.NotEqual(oneBit, eightBits);
        Assert.NotEqual(oneBit, rawBlob);
        Assert.Equal("1", oneBit.AsBitString);
        Assert.Equal("10000000", eightBits.AsBitString);

        DbValue[] decoded = RecordEncoder.Decode(RecordEncoder.Encode([oneBit, eightBits, rawBlob]));
        Assert.Equal(oneBit, decoded[0]);
        Assert.Equal(eightBits, decoded[1]);
        Assert.Equal(rawBlob, decoded[2]);
        Assert.True(decoded[0].IsBitString);
        Assert.Equal(1, decoded[0].BitLength);
        Assert.True(decoded[1].IsBitString);
        Assert.Equal(8, decoded[1].BitLength);
        Assert.False(decoded[2].IsBitString);
    }

    [Fact]
    public void HashedIndexPayloads_PreserveAndCompareExactBitLengths()
    {
        DbValue oneBit = DbValue.FromBitString([0x80], 1);
        DbValue eightBits = DbValue.FromBitString([0x80], 8);

        byte[] bucket = HashedIndexPayloadCodec.CreateSingle([oneBit], rowId: 41);
        Assert.True(HashedIndexPayloadCodec.TryGetMatchingRowIds(bucket, [oneBit], out byte[]? matchingRows));
        Assert.Equal(41, RowIdPayloadCodec.ReadAt(matchingRows!, 0));
        Assert.True(HashedIndexPayloadCodec.TryGetMatchingRowIds(bucket, [eightBits], out byte[]? differentRows));
        Assert.Null(differentRows);

        byte[] appendable = AppendableHashedIndexPayloadCodec.Encode(
            [oneBit],
            firstPageId: 7,
            lastPageId: 7,
            rowCount: 1,
            lastRowId: 41,
            isSortedAscending: true);
        Assert.True(AppendableHashedIndexPayloadCodec.TryDecode(appendable, out var decoded));
        Assert.Equal(oneBit, Assert.Single(decoded.KeyComponents));
        Assert.True(AppendableHashedIndexPayloadCodec.TryDecodeMetadata(appendable, out var metadata));
        Assert.True(AppendableHashedIndexPayloadCodec.EncodedKeyComponentsEqual(
            appendable.AsSpan(metadata.KeyComponentsOffset),
            [oneBit]));
        Assert.False(AppendableHashedIndexPayloadCodec.EncodedKeyComponentsEqual(
            appendable.AsSpan(metadata.KeyComponentsOffset),
            [eightBits]));
    }

    [Fact]
    public async Task BitAndVarBit_PreserveLengthsAcrossUpdateReopenAndIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_bit_strings_{Guid.NewGuid():N}.db");

        try
        {
            await using (Database database = await Database.OpenAsync(path, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE bit_values (" +
                    "id INTEGER PRIMARY KEY, fixed_bits BIT(3), " +
                    "varying_bits VARBIT(8), note TEXT)",
                    ct);
                await database.ExecuteAsync(
                    "CREATE INDEX ix_bit_values_fixed ON bit_values(fixed_bits)",
                    ct);
                await database.ExecuteAsync(
                    "CREATE INDEX ix_bit_values_varying ON bit_values(varying_bits)",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO bit_values VALUES " +
                    "(1, '1', '1', 'before'), " +
                    "(2, '101', '10000000', 'other')",
                    ct);

                // Revalidating the unchanged BIT(3) column must use its three-bit
                // logical length, rather than treating its one packed byte as 8 bits.
                await database.ExecuteAsync(
                    "UPDATE bit_values SET note = 'after' WHERE id = 1",
                    ct);
            }

            await using Database reopened = await Database.OpenAsync(path, ct);
            await using QueryResult scan = await reopened.ExecuteAsync(
                "SELECT fixed_bits, varying_bits, note FROM bit_values ORDER BY id",
                ct);
            List<DbValue[]> rows = await scan.ToListAsync(ct);

            Assert.Equal(2, rows.Count);
            Assert.True(rows[0][0].IsBitString);
            Assert.Equal(3, rows[0][0].BitLength);
            Assert.Equal("100", rows[0][0].AsBitString);
            Assert.True(rows[0][1].IsBitString);
            Assert.Equal(1, rows[0][1].BitLength);
            Assert.Equal("1", rows[0][1].AsBitString);
            Assert.Equal("after", rows[0][2].AsText);
            Assert.True(rows[1][1].IsBitString);
            Assert.Equal(8, rows[1][1].BitLength);
            Assert.Equal("10000000", rows[1][1].AsBitString);
            Assert.NotEqual(rows[0][1], rows[1][1]);
            Assert.Equal(rows[0][1].AsBlob, rows[1][1].AsBlob);

            await AssertIndexedLookupAsync(reopened, "fixed_bits = '1'", 1, ct);
            await AssertIndexedLookupAsync(reopened, "varying_bits = '1'", 1, ct);
            await AssertIndexedLookupAsync(reopened, "varying_bits = '10000000'", 2, ct);

            await using QueryResult castResult = await reopened.ExecuteAsync(
                "SELECT CAST(varying_bits AS BLOB), CAST(varying_bits AS TEXT) " +
                "FROM bit_values WHERE id = 1",
                ct);
            DbValue[] castRow = Assert.Single(await castResult.ToListAsync(ct));
            Assert.False(castRow[0].IsBitString);
            Assert.Equal(new byte[] { 0x80 }, castRow[0].AsBlob);
            Assert.Equal("1", castRow[1].AsText);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
    }

    private static async Task AssertIndexedLookupAsync(
        Database database,
        string predicate,
        long expectedId,
        CancellationToken ct)
    {
        await using QueryResult result = await database.ExecuteAsync(
            $"SELECT id FROM bit_values WHERE {predicate}",
            ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(expectedId, row[0].AsInteger);
    }
}
