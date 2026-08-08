using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class SqlDataTypeIntegrationTests
{
    [Fact]
    public async Task DeclaredSqlTypes_CanonicalizePersistAndMaterializeValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_sql_types_{Guid.NewGuid():N}.db");
        Guid uuid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

        try
        {
            await using (Database database = await Database.OpenAsync(path, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE typed_values (" +
                    "id INTEGER PRIMARY KEY, " +
                    "flag BOOLEAN, tiny TINYINT, small SMALLINT, big BIGINT, " +
                    "single_value REAL, double_value DOUBLE PRECISION, " +
                    "amount DECIMAL(18,4), fixed_text CHAR(4), varying_text VARCHAR(5), note TEXT, " +
                    "fixed_bytes BINARY(4), varying_bytes VARBINARY(4), raw_bytes BLOB, uid UUID, " +
                    "day_value DATE, clock_value TIME(3), stamp_value DATETIME2(3), " +
                    "zoned_value DATETIMEOFFSET(3), " +
                    "year_month INTERVAL YEAR TO MONTH, day_second INTERVAL DAY TO SECOND, " +
                    "document JSON, markup XML, fixed_bits BIT(4), varying_bits BIT VARYING(8))",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO typed_values VALUES (" +
                    "1, 1, 255, -32768, 9223372036854775807, 1.25, 2.5, 123.4500, " +
                    "'ab', 'hello', 'notes', X'0102', X'0304', X'05', " +
                    $"'{uuid:D}', " +
                    "'2026-08-05', '12:34:56.123', '2026-08-05 12:34:56.123', " +
                    "'2026-08-05 12:34:56.123-07:00', '2-03', '1.02:03:04.5', " +
                    "'{\"answer\": 42}', '<root><answer>42</answer></root>', '1010', '101')",
                    ct);
            }

            await using Database reopened = await Database.OpenAsync(path, ct);
            TableSchema schema = reopened.GetTableSchema("typed_values")!;
            Assert.Equal(SqlTypeKind.Boolean, schema.Columns[1].EffectiveType.Kind);
            Assert.Equal(SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 18, scale: 4), schema.Columns[7].DeclaredType);
            Assert.Equal(SqlTypeKind.TimestampWithTimeZone, schema.Columns[18].EffectiveType.Kind);
            Assert.Equal(SqlTypeDescriptor.Create(SqlTypeKind.VarBit, length: 8), schema.Columns[24].DeclaredType);

            await using QueryResult scanResult = await reopened.ExecuteAsync(
                "SELECT * FROM typed_values",
                ct);
            Assert.Single(await scanResult.ToListAsync(ct));

            await using QueryResult result = await reopened.ExecuteAsync(
                "SELECT * FROM typed_values WHERE id = 1",
                ct);
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(25, row.Length);
            Assert.Equal(1L, row[1].AsInteger);
            Assert.Equal(255L, row[2].AsInteger);
            Assert.Equal(-32768L, row[3].AsInteger);
            Assert.Equal(long.MaxValue, row[4].AsInteger);
            Assert.Equal(1.25d, row[5].AsReal);
            Assert.Equal(2.5d, row[6].AsReal);
            Assert.Equal(123.45m, row[7].AsDecimal);
            Assert.Equal("ab  ", row[8].AsText);
            Assert.Equal("hello", row[9].AsText);
            Assert.Equal(new byte[] { 0x01, 0x02, 0x00, 0x00 }, row[11].AsBlob);
            Assert.Equal(new byte[] { 0x03, 0x04 }, row[12].AsBlob);
            Assert.Equal(uuid, new Guid(row[14].AsBlob, bigEndian: true));
            Assert.Equal("2026-08-05", row[15].AsText);
            Assert.Equal("12:34:56.123", row[16].AsText);
            Assert.Equal("2026-08-05 12:34:56.123", row[17].AsText);
            Assert.Equal("2026-08-05 19:34:56.123+00:00", row[18].AsText);
            Assert.Equal("2-03", row[19].AsText);
            Assert.Equal(TimeSpan.Parse("1.02:03:04.5"), TimeSpan.Parse(row[20].AsText));
            Assert.Equal("{\"answer\":42}", row[21].AsText);
            Assert.Equal("<root><answer>42</answer></root>", row[22].AsText);
            Assert.Equal(new byte[] { 0xA0 }, row[23].AsBlob);
            Assert.Equal(new byte[] { 0xA0 }, row[24].AsBlob);

            await using QueryResult zonedCast = await reopened.ExecuteAsync(
                "SELECT CAST('2026-08-05 12:34:56.123-07:00' AS DATETIMEOFFSET(3))",
                ct);
            Assert.Equal(
                row[18].AsText,
                Assert.Single(await zonedCast.ToListAsync(ct))[0].AsText);

            foreach (string predicate in new[]
                     {
                         $"uid IN ('{uuid:D}')",
                         "zoned_value = '2026-08-05 12:34:56.123-07:00'",
                         "amount BETWEEN 123.4500 AND 123.4500",
                     })
            {
                await using QueryResult typedPredicate = await reopened.ExecuteAsync(
                    $"SELECT id FROM typed_values WHERE {predicate}",
                    ct);
                List<DbValue[]> predicateRows = await typedPredicate.ToListAsync(ct);
                Assert.True(
                    predicateRows.Count == 1,
                    $"Typed predicate '{predicate}' returned {predicateRows.Count} rows.");
                Assert.Equal(
                    1L,
                    predicateRows[0][0].AsInteger);
            }

            await using QueryResult catalog = await reopened.ExecuteAsync(
                "SELECT data_type FROM sys.columns " +
                "WHERE table_name = 'typed_values' ORDER BY ordinal_position",
                ct);
            string[] declaredTypes = (await catalog.ToListAsync(ct))
                .Select(static value => value[0].AsText)
                .ToArray();
            Assert.Contains("DECIMAL(18,4)", declaredTypes);
            Assert.Contains("DATETIMEOFFSET(3)", declaredTypes);
            Assert.Contains("BIT VARYING(8)", declaredTypes);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task Cast_UsesDeclaredTargetAndPreservesExactNumericText()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await using QueryResult result = await database.ExecuteAsync(
            "SELECT CAST('true' AS BOOLEAN) AS flag, " +
            "CAST(123.4500 AS DECIMAL(18,4)) AS amount, " +
            "CAST(9999999999999999.99 AS DECIMAL(18,2)) AS positive_limit, " +
            "CAST(-9999999999999999.99 AS DECIMAL(18,2)) AS negative_limit, " +
            "CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID) AS uid",
            ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(1L, row[0].AsInteger);
        Assert.Equal(123.45m, row[1].AsDecimal);
        Assert.Equal(9999999999999999.99m, row[2].AsDecimal);
        Assert.Equal(-9999999999999999.99m, row[3].AsDecimal);
        Assert.Equal(
            Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"),
            new Guid(row[4].AsBlob, bigEndian: true));
        Assert.Equal(SqlTypeKind.Boolean, result.Schema[0].EffectiveType.Kind);
        Assert.Equal(SqlTypeKind.Decimal, result.Schema[1].EffectiveType.Kind);
        Assert.Equal(SqlTypeKind.Decimal, result.Schema[2].EffectiveType.Kind);
        Assert.Equal(SqlTypeKind.Decimal, result.Schema[3].EffectiveType.Kind);
        Assert.Equal(SqlTypeKind.Uuid, result.Schema[4].EffectiveType.Kind);
    }

    [Fact]
    public async Task BigInt_MinimumLiteralIsRepresentableAndNegationOverflows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);

        await using QueryResult literalResult = await database.ExecuteAsync(
            "SELECT CAST(-9223372036854775808 AS BIGINT)",
            ct);
        Assert.Equal(
            long.MinValue,
            Assert.Single(await literalResult.ToListAsync(ct))[0].AsInteger);

        Assert.Throws<OverflowException>(() =>
            ExpressionEvaluator.NegateNumeric(DbValue.FromInteger(long.MinValue)));
    }

    [Fact]
    public async Task Cast_UsesLogicalSourceTypesForUuidBooleanAndTemporalFamilies()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE cast_sources (" +
            "flag BOOLEAN, uid UUID, day_value DATE, " +
            "stamp_value DATETIME2(7), zoned_value DATETIMEOFFSET(7))",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO cast_sources VALUES (" +
            "1, '00112233-4455-6677-8899-aabbccddeeff', '2026-08-05', " +
            "'2026-08-05 14:30:15.1234567', " +
            "'2026-08-05 14:30:15.1234567-07:00')",
            ct);

        await using QueryResult result = await database.ExecuteAsync(
            "SELECT CAST(flag AS TEXT), CAST(uid AS TEXT), " +
            "CAST(day_value AS DATETIME2(7)), " +
            "CAST(stamp_value AS DATE), CAST(stamp_value AS TIME(7)), " +
            "CAST(stamp_value AS DATETIMEOFFSET(7)), " +
            "CAST(zoned_value AS DATETIME2(7)), " +
            "CAST(zoned_value AS DATE), CAST(zoned_value AS TIME(7)) " +
            "FROM cast_sources",
            ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal("TRUE", row[0].AsText);
        Assert.Equal("00112233-4455-6677-8899-aabbccddeeff", row[1].AsText);
        Assert.Equal("2026-08-05 00:00:00", row[2].AsText);
        Assert.Equal("2026-08-05", row[3].AsText);
        Assert.Equal("14:30:15.1234567", row[4].AsText);
        Assert.Equal("2026-08-05 14:30:15.1234567+00:00", row[5].AsText);
        Assert.Equal("2026-08-05 21:30:15.1234567", row[6].AsText);
        Assert.Equal("2026-08-05", row[7].AsText);
        Assert.Equal("21:30:15.1234567", row[8].AsText);
    }

    [Fact]
    public async Task DeclaredFacetsAndDomains_RejectInvalidAssignmentsAtomically()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE guarded_types (" +
            "id INTEGER PRIMARY KEY, tiny TINYINT, code VARCHAR(3), " +
            "amount DECIMAL(5,2), day_value DATE, document JSON)",
            ct);

        string[] invalidInserts =
        [
            "INSERT INTO guarded_types VALUES (1, 256, 'ok', 1.00, '2026-08-05', '{}')",
            "INSERT INTO guarded_types VALUES (2, 1, 'long', 1.00, '2026-08-05', '{}')",
            "INSERT INTO guarded_types VALUES (3, 1, 'ok', 1.001, '2026-08-05', '{}')",
            "INSERT INTO guarded_types VALUES (4, 1, 'ok', 1.00, 'not-a-date', '{}')",
            "INSERT INTO guarded_types VALUES (5, 1, 'ok', 1.00, '2026-08-05', '{bad}')",
        ];

        foreach (string sql in invalidInserts)
        {
            CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await database.ExecuteAsync(sql, ct));
            Assert.Equal(ErrorCode.TypeMismatch, failure.Code);
        }

        await using QueryResult count = await database.ExecuteAsync(
            "SELECT COUNT(*) FROM guarded_types",
            ct);
        Assert.Equal(0L, Assert.Single(await count.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task DecimalUuidAndBlob_EqualityIndexesAndAggregatesRemainExact()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE indexed_types (" +
            "id INTEGER PRIMARY KEY, amount DECIMAL(18,4), uid UUID, payload BLOB)",
            ct);
        await database.ExecuteAsync("CREATE INDEX ix_indexed_types_amount ON indexed_types(amount)", ct);
        await database.ExecuteAsync("CREATE INDEX ix_indexed_types_uid ON indexed_types(uid)", ct);
        await database.ExecuteAsync("CREATE INDEX ix_indexed_types_payload ON indexed_types(payload)", ct);
        await database.ExecuteAsync(
            "INSERT INTO indexed_types VALUES " +
            "(1, 1.2300, '00112233-4455-6677-8899-aabbccddeeff', X'0102'), " +
            "(2, 2.3400, '11112233-4455-6677-8899-aabbccddeeff', X'0304'), " +
            "(3, -1.2300, '22112233-4455-6677-8899-aabbccddeeff', X'0102')",
            ct);

        await using QueryResult amountLookup = await database.ExecuteAsync(
            "SELECT id FROM indexed_types WHERE amount = 1.2300",
            ct);
        Assert.Equal(1L, Assert.Single(await amountLookup.ToListAsync(ct))[0].AsInteger);

        await using QueryResult uuidLookup = await database.ExecuteAsync(
            "SELECT id FROM indexed_types " +
            "WHERE uid = '11112233-4455-6677-8899-aabbccddeeff'",
            ct);
        Assert.Equal(2L, Assert.Single(await uuidLookup.ToListAsync(ct))[0].AsInteger);

        await using QueryResult blobLookup = await database.ExecuteAsync(
            "SELECT id FROM indexed_types WHERE payload = X'0102' ORDER BY id",
            ct);
        long[] blobMatches = (await blobLookup.ToListAsync(ct))
            .Select(static row => row[0].AsInteger)
            .ToArray();
        Assert.Equal(
            new long[] { 1L, 3L },
            blobMatches);

        await using QueryResult noMatch = await database.ExecuteAsync(
            "SELECT id FROM indexed_types WHERE amount = 1.2301",
            ct);
        Assert.Empty(await noMatch.ToListAsync(ct));

        await using QueryResult aggregates = await database.ExecuteAsync(
            "SELECT SUM(amount), AVG(amount) FROM indexed_types",
            ct);
        DbValue[] aggregateRow = Assert.Single(await aggregates.ToListAsync(ct));
        Assert.Equal(2.34m, aggregateRow[0].AsDecimal);
        Assert.Equal(0.78m, aggregateRow[1].AsDecimal);
        Assert.All(aggregates.Schema, column => Assert.Equal(DbType.Decimal, column.Type));
    }

    [Fact]
    public async Task ExplicitCteColumnNames_PreserveLogicalTypesAndTypedPredicates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE cte_types (" +
            "id INTEGER PRIMARY KEY, amount DECIMAL(18,4), uid UUID, " +
            "zoned DATETIMEOFFSET(3))",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO cte_types VALUES (" +
            "1, 123.4500, '00112233-4455-6677-8899-aabbccddeeff', " +
            "'2026-08-05 12:34:56.123-07:00')",
            ct);

        await using QueryResult result = await database.ExecuteAsync(
            "WITH typed(alias_id, alias_amount, alias_uid, alias_zoned) AS (" +
            "SELECT id, amount, uid, zoned FROM cte_types) " +
            "SELECT alias_amount FROM typed " +
            "WHERE alias_uid = '00112233-4455-6677-8899-AABBCCDDEEFF' " +
            "AND alias_zoned = '2026-08-05 12:34:56.123-07:00'",
            ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(123.45m, row[0].AsDecimal);
        Assert.Equal(
            SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 18, scale: 4),
            result.Schema[0].DeclaredType);
    }

    [Fact]
    public async Task ExactDecimalDefaults_PreserveParsedTextAcrossReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_decimal_defaults_{Guid.NewGuid():N}.db");

        try
        {
            await using (Database database = await Database.OpenAsync(path, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE decimal_defaults (" +
                    "id INTEGER PRIMARY KEY, " +
                    "positive DECIMAL(18,2) DEFAULT 1234567890123456.78, " +
                    "negative DECIMAL(18,2) DEFAULT -1234567890123456.78)",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO decimal_defaults (id) VALUES (1)",
                    ct);
            }

            await using Database reopened = await Database.OpenAsync(path, ct);
            TableSchema schema = reopened.GetTableSchema("decimal_defaults")!;
            Assert.Equal("1234567890123456.78", schema.Columns[1].DefaultSql);
            Assert.Equal("-1234567890123456.78", schema.Columns[2].DefaultSql);

            await using QueryResult result = await reopened.ExecuteAsync(
                "SELECT positive, negative FROM decimal_defaults",
                ct);
            DbValue[] row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal(1234567890123456.78m, row[0].AsDecimal);
            Assert.Equal(-1234567890123456.78m, row[1].AsDecimal);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task DecimalAndUuid_SubqueriesPreserveExactTypedValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE subquery_types (" +
            "id INTEGER PRIMARY KEY, amount DECIMAL(18,2), uid UUID)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO subquery_types VALUES " +
            "(1, 1234567890123456.78, '00112233-4455-6677-8899-aabbccddeeff'), " +
            "(2, 1.00, '11112233-4455-6677-8899-aabbccddeeff')",
            ct);

        await using QueryResult decimalIn = await database.ExecuteAsync(
            "SELECT id FROM subquery_types WHERE amount IN (" +
            "SELECT amount FROM subquery_types WHERE id = 1)",
            ct);
        Assert.Equal(1L, Assert.Single(await decimalIn.ToListAsync(ct))[0].AsInteger);

        await using QueryResult uuidIn = await database.ExecuteAsync(
            "SELECT id FROM subquery_types WHERE uid IN (" +
            "SELECT uid FROM subquery_types WHERE id = 2)",
            ct);
        Assert.Equal(2L, Assert.Single(await uuidIn.ToListAsync(ct))[0].AsInteger);

        await using QueryResult scalar = await database.ExecuteAsync(
            "SELECT (SELECT amount FROM subquery_types WHERE id = 1) AS exact_amount",
            ct);
        Assert.Equal(
            1234567890123456.78m,
            Assert.Single(await scalar.ToListAsync(ct))[0].AsDecimal);
        Assert.Equal(
            SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 18, scale: 2),
            scalar.Schema[0].DeclaredType);
    }

    [Fact]
    public async Task ComputedExpressionsExposeSafeLogicalResultTypes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE expression_types (" +
            "amount DECIMAL(4,2) NOT NULL, supplement DECIMAL(3,0) NOT NULL, " +
            "small_value SMALLINT NOT NULL)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO expression_types VALUES (1.20, 2, 3)",
            ct);

        await using QueryResult result = await database.ExecuteAsync(
            "SELECT -amount AS negated, " +
            "amount + supplement AS total, " +
            "amount * supplement AS product, " +
            "amount / supplement AS quotient, " +
            "small_value + small_value AS widened_integer, " +
            "amount > 0 AS compared, " +
            "NOT (amount = 0) AS negated_predicate, " +
            "amount BETWEEN 1 AND 2 AS bounded, " +
            "amount IN (1.20, 2.00) AS listed, " +
            "amount IS NULL AS null_test " +
            "FROM expression_types",
            ct);

        Assert.Equal(
            SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 4, scale: 2),
            result.Schema[0].DeclaredType);
        Assert.Equal(
            SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 6, scale: 2),
            result.Schema[1].DeclaredType);
        Assert.Equal(
            SqlTypeDescriptor.Create(SqlTypeKind.Decimal, precision: 7, scale: 2),
            result.Schema[2].DeclaredType);
        Assert.Equal(DbType.Decimal, result.Schema[3].Type);
        Assert.Null(result.Schema[3].DeclaredType);
        Assert.Equal(SqlTypeKind.BigInt, result.Schema[4].EffectiveType.Kind);
        Assert.All(
            result.Schema.Skip(5),
            column => Assert.Equal(SqlTypeKind.Boolean, column.EffectiveType.Kind));

        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(-1.20m, row[0].AsDecimal);
        Assert.Equal(3.20m, row[1].AsDecimal);
        Assert.Equal(2.40m, row[2].AsDecimal);
        Assert.Equal(0.60m, row[3].AsDecimal);
        Assert.Equal(6L, row[4].AsInteger);
        Assert.Equal([1L, 1L, 1L, 1L, 0L], row.Skip(5).Select(value => value.AsInteger));
    }

    [Fact]
    public async Task SetOperationsWidenDecimalFacetsIndependentOfBranchOrderAndRejectLogicalCollisions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);

        await using QueryResult narrowFirst = await database.ExecuteAsync(
            "SELECT CAST(1.23 AS DECIMAL(4,2)) AS amount " +
            "UNION ALL SELECT CAST(1234.5678 AS DECIMAL(8,4))",
            ct);
        await using QueryResult wideFirst = await database.ExecuteAsync(
            "SELECT CAST(1234.5678 AS DECIMAL(8,4)) AS amount " +
            "UNION ALL SELECT CAST(1.23 AS DECIMAL(4,2))",
            ct);

        SqlTypeDescriptor expected = SqlTypeDescriptor.Create(
            SqlTypeKind.Decimal,
            precision: 8,
            scale: 4);
        Assert.Equal(expected, narrowFirst.Schema[0].DeclaredType);
        Assert.Equal(expected, wideFirst.Schema[0].DeclaredType);

        string[] incompatibleQueries =
        [
            "SELECT CAST(1 AS BOOLEAN) UNION ALL SELECT CAST(1 AS BIGINT)",
            "SELECT CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID) " +
                "UNION ALL SELECT CAST(X'00112233445566778899AABBCCDDEEFF' AS BLOB)",
        ];
        foreach (string query in incompatibleQueries)
        {
            CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await database.ExecuteAsync(query, ct));
            Assert.Equal(ErrorCode.TypeMismatch, failure.Code);
            Assert.Contains("incompatible logical types", failure.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Intervals_UseDurationOrderingAcrossFiltersIndexesSortsAndAggregates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE interval_values (" +
            "id INTEGER PRIMARY KEY, " +
            "bucket TEXT, " +
            "year_month INTERVAL YEAR TO MONTH, " +
            "day_second INTERVAL DAY TO SECOND)",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO interval_values VALUES " +
            "(1, 'all', '-1-00', '-1.00:00:00'), " +
            "(2, 'all', '2-00', '2.00:00:00'), " +
            "(3, 'all', '10-00', '10.00:00:00')",
            ct);
        await database.ExecuteAsync(
            "CREATE INDEX ix_interval_values_ym ON interval_values(year_month)",
            ct);
        await database.ExecuteAsync(
            "CREATE INDEX ix_interval_values_ds ON interval_values(day_second)",
            ct);

        await using (QueryResult filtered = await database.ExecuteAsync(
                         "SELECT id FROM interval_values " +
                         "WHERE year_month > '2-00' AND day_second > '2.00:00:00'",
                         ct))
        {
            Assert.Equal(3L, Assert.Single(await filtered.ToListAsync(ct))[0].AsInteger);
        }

        await using (QueryResult between = await database.ExecuteAsync(
                         "SELECT id FROM interval_values " +
                         "WHERE day_second BETWEEN '2.00:00:00' AND '10.00:00:00' ORDER BY id",
                         ct))
        {
            Assert.Equal(
                new long[] { 2, 3 },
                (await between.ToListAsync(ct)).Select(static row => row[0].AsInteger));
        }

        await using (QueryResult ordered = await database.ExecuteAsync(
                         "SELECT id FROM interval_values ORDER BY year_month LIMIT 2",
                         ct))
        {
            Assert.Equal(
                new long[] { 1, 2 },
                (await ordered.ToListAsync(ct)).Select(static row => row[0].AsInteger));
        }

        await using (QueryResult aggregates = await database.ExecuteAsync(
                         "SELECT MIN(year_month), MAX(year_month), " +
                         "MIN(day_second), MAX(day_second) FROM interval_values",
                         ct))
        {
            DbValue[] row = Assert.Single(await aggregates.ToListAsync(ct));
            Assert.Equal("-1-00", row[0].AsText);
            Assert.Equal("10-00", row[1].AsText);
            Assert.Equal(TimeSpan.FromDays(-1), TimeSpan.Parse(row[2].AsText));
            Assert.Equal(TimeSpan.FromDays(10), TimeSpan.Parse(row[3].AsText));
        }

        await using (QueryResult fastMaximum = await database.ExecuteAsync(
                         "SELECT MAX(year_month) FROM interval_values",
                         ct))
        {
            Assert.Equal("10-00", Assert.Single(await fastMaximum.ToListAsync(ct))[0].AsText);
        }

        await using (QueryResult filteredFastMaximum = await database.ExecuteAsync(
                         "SELECT MAX(year_month) FROM interval_values WHERE id > 0",
                         ct))
        {
            Assert.Equal("10-00", Assert.Single(await filteredFastMaximum.ToListAsync(ct))[0].AsText);
        }

        await using (QueryResult filteredFastMinimum = await database.ExecuteAsync(
                         "SELECT MIN(day_second) FROM interval_values WHERE id > 0",
                         ct))
        {
            Assert.Equal(
                TimeSpan.FromDays(-1),
                TimeSpan.Parse(Assert.Single(await filteredFastMinimum.ToListAsync(ct))[0].AsText));
        }

        await using (QueryResult groupedMaximum = await database.ExecuteAsync(
                         "SELECT bucket, MAX(year_month) FROM interval_values GROUP BY bucket",
                         ct))
        {
            DbValue[] row = Assert.Single(await groupedMaximum.ToListAsync(ct));
            Assert.Equal("all", row[0].AsText);
            Assert.Equal("10-00", row[1].AsText);
        }

        await using (QueryResult joined = await database.ExecuteAsync(
                         "SELECT lhs.id, rhs.id FROM interval_values lhs " +
                         "JOIN interval_values rhs ON lhs.year_month < rhs.year_month " +
                         "WHERE lhs.id = 2 AND rhs.id = 3",
                         ct))
        {
            DbValue[] row = Assert.Single(await joined.ToListAsync(ct));
            Assert.Equal(2L, row[0].AsInteger);
            Assert.Equal(3L, row[1].AsInteger);
        }
    }

    [Fact]
    public async Task TypedCastExpressions_CoerceUntypedComparisonValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        const string uuid = "00112233-4455-6677-8899-aabbccddeeff";

        await using QueryResult result = await database.ExecuteAsync(
            $"SELECT " +
            $"CAST('{uuid}' AS UUID) = '{uuid}', " +
            $"'{uuid}' = CAST('{uuid}' AS UUID), " +
            $"CAST('{uuid}' AS UUID) IN ('{uuid}'), " +
            $"CAST('{uuid}' AS UUID) BETWEEN " +
            $"'00000000-0000-0000-0000-000000000000' AND " +
            $"'ffffffff-ffff-ffff-ffff-ffffffffffff', " +
            $"CAST('101' AS BIT VARYING(3)) = '101', " +
            $"CAST('ab' AS BINARY(2)) = X'6162'",
            ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.All(row, static value => Assert.Equal(1L, value.AsInteger));
        Assert.All(
            result.Schema,
            static column => Assert.Equal(SqlTypeKind.Boolean, column.EffectiveType.Kind));
    }
}
