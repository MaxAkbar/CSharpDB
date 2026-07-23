using System.Globalization;
using System.Reflection;
using System.Text;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvStreamingReaderTests
{
    [Fact]
    public async Task ReadsQuotedMultilineFieldsAndEscapedQuotes()
    {
        const string csv =
            "id,notes,quote\r\n" +
            "1,\"first\r\nsecond\",\"He said \"\"hello\"\"\"\r\n" +
            "2,plain,end";

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        List<CsvLogicalRecord> records = await CollectAsync(ReadRecords(reader));

        Assert.NotNull(reader.Header);
        Assert.Equal(["id", "notes", "quote"], reader.Header.Fields);
        Assert.Equal(1, reader.Header.StartPhysicalLine);
        Assert.Equal(1, reader.Header.EndPhysicalLine);
        Assert.Equal(2, records.Count);

        Assert.Equal(2, records[0].LogicalRecordNumber);
        Assert.Equal(1, records[0].DataRecordNumber);
        Assert.Equal(2, records[0].StartPhysicalLine);
        Assert.Equal(3, records[0].EndPhysicalLine);
        Assert.Equal("first\r\nsecond", records[0].Fields[1].Value);
        Assert.Equal("He said \"hello\"", records[0].Fields[2].Value);

        Assert.Equal(4, records[1].StartPhysicalLine);
        Assert.Equal(4, records[1].EndPhysicalLine);
        Assert.Equal("plain", records[1].Fields[1].Value);
    }

    [Fact]
    public async Task KeepsNullEmptyMissingAndTrailingEmptyFieldsDistinct()
    {
        const string csv = "a,b,c,d\n\\N,,value,\nx,y";
        var options = new CsvReaderOptions { NullToken = "\\N" };

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);
        List<CsvLogicalRecord> records = await CollectAsync(ReadRecords(reader));

        Assert.Equal(4, records[0].PresentFieldCount);
        Assert.Equal(
            [CsvFieldKind.Null, CsvFieldKind.Empty, CsvFieldKind.Text, CsvFieldKind.Empty],
            records[0].Fields.Select(field => field.Kind));
        Assert.Null(records[0].Fields[0].Value);
        Assert.Equal(string.Empty, records[0].Fields[1].Value);
        Assert.Equal(string.Empty, records[0].Fields[3].Value);

        Assert.Equal(2, records[1].PresentFieldCount);
        Assert.Equal(
            [CsvFieldKind.Text, CsvFieldKind.Text, CsvFieldKind.Missing, CsvFieldKind.Missing],
            records[1].Fields.Select(field => field.Kind));
        Assert.Null(records[1].Fields[2].Value);
        Assert.Null(records[1].Fields[3].Value);
    }

    [Fact]
    public async Task PreservesBlankAndDuplicateHeaderNames()
    {
        await using var stream = Utf8("name,,name\nvalue,,other");
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);

        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal(["name", "", "name"], reader.Header!.Fields);
        Assert.Equal(CsvFieldKind.Empty, record.Fields[1].Kind);
    }

    [Theory]
    [InlineData("\n")]
    [InlineData("\r")]
    [InlineData("\r\n")]
    public async Task ReadsCommonRecordTerminatorsAndBlankRecords(string newline)
    {
        string csv = $"a,b{newline}1,2{newline}{newline}3,4";
        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);

        List<CsvLogicalRecord> records = await CollectAsync(ReadRecords(reader));

        Assert.Equal(3, records.Count);
        Assert.Equal(CsvFieldKind.Empty, records[1].Fields[0].Kind);
        Assert.Equal(CsvFieldKind.Missing, records[1].Fields[1].Kind);
        Assert.Equal("3", records[2].Fields[0].Value);
    }

    [Fact]
    public async Task NullTokenPreservesAQuotedLiteralByDefault()
    {
        const string csv = "a,b\nNULL,\"NULL\"";
        var options = new CsvReaderOptions { NullToken = "NULL" };

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);
        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvFieldKind.Null, record.Fields[0].Kind);
        Assert.False(record.Fields[0].WasQuoted);
        Assert.Equal(CsvFieldKind.Text, record.Fields[1].Kind);
        Assert.Equal("NULL", record.Fields[1].Value);
        Assert.True(record.Fields[1].WasQuoted);
    }

    [Fact]
    public async Task NullTokenCanExplicitlyMatchQuotedFields()
    {
        await using var stream = Utf8("a\n\"NULL,VALUE\"");
        var options = new CsvReaderOptions
        {
            NullToken = "NULL,VALUE",
            NullTokenMatchesQuotedFields = true,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvFieldKind.Null, record.Fields[0].Kind);
        Assert.True(record.Fields[0].WasQuoted);
    }

    [Fact]
    public async Task ReversibleNullTokenRejectsStructuralCharactersBeforeReading()
    {
        MemoryStream source = Utf8("a\n1");
        var options = new CsvReaderOptions
        {
            NullToken = "NULL,VALUE",
            LeaveOpen = true,
        };

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await OpenReaderAsync(source, options).AsTask());

        Assert.Equal(0, source.Position);
        source.Dispose();
    }

    [Fact]
    public async Task PreservesWhitespaceAndCultureLookingLexemes()
    {
        const string csv = "code;amount;date\n 001 ;1,25;31.12.2026";
        var options = new CsvReaderOptions
        {
            Delimiter = ";",
            Culture = CultureInfo.GetCultureInfo("de-DE"),
        };

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);
        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal(" 001 ", record.Fields[0].Value);
        Assert.Equal("1,25", record.Fields[1].Value);
        Assert.Equal("31.12.2026", record.Fields[2].Value);
        Assert.Equal("de-DE", reader.CultureName);
    }

    [Fact]
    public async Task SupportsACustomQuoteAcrossOneByteReads()
    {
        const string csv = "id,notes\r\n1,'first\r\nsecond ''quote'''";
        await using var inner = Utf8(csv);
        await using var stream = new ChunkedReadStream(inner, 1);
        var options = new CsvReaderOptions { Quote = '\'' };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal('\'', reader.Quote);
        Assert.Equal("first\r\nsecond 'quote'", record.Fields[1].Value);
        Assert.True(record.Fields[1].WasQuoted);
    }

    [Fact]
    public async Task HeaderlessInputUsesExplicitWidthAndPadsMissingFields()
    {
        const string csv = "a,b\nc,d,e";
        var options = new CsvReaderOptions
        {
            HasHeaderRecord = false,
            ExpectedFieldCount = 3,
        };

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);
        List<CsvLogicalRecord> records = await CollectAsync(ReadRecords(reader));

        Assert.Null(reader.Header);
        Assert.Equal(3, reader.FieldCount);
        Assert.Equal(CsvFieldKind.Missing, records[0].Fields[2].Kind);
        Assert.Equal(3, records[1].PresentFieldCount);
    }

    [Fact]
    public async Task HeaderlessInputFreezesWidthFromFirstRecord()
    {
        const string csv = "a,b,c\nd,e";
        var options = new CsvReaderOptions { HasHeaderRecord = false };

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);
        Assert.Null(reader.FieldCount);

        List<CsvLogicalRecord> records = await CollectAsync(ReadRecords(reader));

        Assert.Equal(3, reader.FieldCount);
        Assert.Equal(CsvFieldKind.Missing, records[1].Fields[2].Kind);
    }

    [Fact]
    public async Task RejectsExtraFieldsWithoutIncludingValuesInTheError()
    {
        const string secret = "do-not-leak-this-value";
        string csv = $"a,b\n1,2,{secret}";

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.ExtraFields, exception.Diagnostic.RuleId);
        Assert.Equal(2, exception.Diagnostic.ColumnIndex);
        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RejectsMalformedQuotesWithoutIncludingRawValues()
    {
        const string secret = "sensitive-value";
        string csv = $"a,b\n1,unquoted\"{secret}";

        await using var stream = Utf8(csv);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.MalformedData, exception.Diagnostic.RuleId);
        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RejectsAnUnclosedQuotedFieldAtEndOfInput()
    {
        await using var stream = Utf8("a,b\n1,\"not closed");
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);

        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.MalformedData, exception.Diagnostic.RuleId);
    }

    [Fact]
    public async Task MalformedMultilineDiagnosticUsesTheWholeLogicalRecordRange()
    {
        await using var stream = Utf8("a,b\n1,\"line1\nline2\"junk");
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);

        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.MalformedData, exception.Diagnostic.RuleId);
        Assert.Equal(2, exception.Diagnostic.LogicalRecordNumber);
        Assert.Equal(1, exception.Diagnostic.DataRecordNumber);
        Assert.Equal(2, exception.Diagnostic.StartPhysicalLine);
        Assert.Equal(3, exception.Diagnostic.EndPhysicalLine);
    }

    [Fact]
    public async Task MultilineLimitDiagnosticUsesThePhysicalFailureLine()
    {
        await using var stream = Utf8("a,b\n1,\"ab\ncdef\"");
        var options = new CsvReaderOptions
        {
            MaxFieldCharacters = 5,
            MaxRecordCharacters = 32,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.FieldLimitExceeded, exception.Diagnostic.RuleId);
        Assert.Equal(2, exception.Diagnostic.StartPhysicalLine);
        Assert.Equal(3, exception.Diagnostic.EndPhysicalLine);
    }

    [Fact]
    public async Task EnforcesFieldRecordAndColumnLimits()
    {
        await AssertRuleAsync(
            "a\n12345",
            new CsvReaderOptions { MaxFieldCharacters = 4, MaxRecordCharacters = 32 },
            CsvDiagnosticRules.FieldLimitExceeded);

        await AssertRuleAsync(
            "a\n1,2,3,4",
            new CsvReaderOptions
            {
                ExpectedFieldCount = 1,
                MaxFieldCharacters = 4,
                MaxRecordCharacters = 6,
            },
            CsvDiagnosticRules.RecordLimitExceeded);

        await AssertRuleAsync(
            "a,b,c\n1,2,3",
            new CsvReaderOptions
            {
                MaxFieldsPerRecord = 2,
                MaxFieldCharacters = 4,
                MaxRecordCharacters = 32,
            },
            CsvDiagnosticRules.FieldCountLimitExceeded,
            failsWhileOpening: true);
    }

    [Fact]
    public async Task AcceptsExactFieldAndRecordLimits()
    {
        await using var stream = Utf8("a\n1234");
        var options = new CsvReaderOptions
        {
            MaxFieldCharacters = 4,
            MaxRecordCharacters = 4,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal("1234", record.Fields[0].Value);
    }

    [Fact]
    public async Task FieldLimitCountsDecodedQuotedAndEscapedValues()
    {
        await using var stream = Utf8("a\n\"a\"\n\"\"\"\"");
        var options = new CsvReaderOptions
        {
            MaxFieldCharacters = 1,
            MaxRecordCharacters = 5,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        List<CsvLogicalRecord> records = await CollectAsync(ReadRecords(reader));

        Assert.Equal(2, records.Count);
        Assert.Equal("a", records[0].Fields[0].Value);
        Assert.Equal("\"", records[1].Fields[0].Value);
        Assert.All(records, record => Assert.True(record.Fields[0].WasQuoted));
    }

    [Fact]
    public async Task RecordLimitCountsDecodedCsvSyntaxIncludingQuotes()
    {
        await using var stream = Utf8("a\n\"a\"");
        var options = new CsvReaderOptions
        {
            MaxFieldCharacters = 1,
            MaxRecordCharacters = 3,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal("a", record.Fields[0].Value);
        Assert.True(record.Fields[0].WasQuoted);

        await using var rejectedStream = Utf8("a\n\"a\"");
        var rejectedOptions = options with { MaxRecordCharacters = 2 };
        await using CsvStreamingReader rejectedReader = await OpenReaderAsync(
            rejectedStream,
            rejectedOptions);
        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(rejectedReader)));
        Assert.Equal(CsvDiagnosticRules.RecordLimitExceeded, exception.Diagnostic.RuleId);
    }

    [Fact]
    public async Task BoundsARecordContainingManySmallFieldsBeforeMaterialization()
    {
        string csv = "a\n" + string.Join(',', Enumerable.Repeat("x", 10_000));
        await using var stream = Utf8(csv);
        var options = new CsvReaderOptions
        {
            MaxFieldCharacters = 1,
            MaxRecordCharacters = 128,
            MaxFieldsPerRecord = CsvReaderOptions.MaximumSupportedFieldsPerRecord,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.RecordLimitExceeded, exception.Diagnostic.RuleId);
    }

    [Fact]
    public async Task EnforcesColumnLimitBeforeAHostileDelimiterRecordIsMaterialized()
    {
        string csv = "a\n" + new string(',', 100_000);
        await using var stream = Utf8(csv);
        var options = new CsvReaderOptions
        {
            MaxFieldCharacters = 1,
            MaxRecordCharacters = 200_000,
            MaxFieldsPerRecord = 32,
        };
        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);

        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));

        Assert.Equal(CsvDiagnosticRules.FieldCountLimitExceeded, exception.Diagnostic.RuleId);
        Assert.Equal(32, exception.Diagnostic.ColumnIndex);
    }

    [Fact]
    public async Task RejectsConfiguredLimitsAboveAbsoluteSafetyCeilings()
    {
        CsvReaderOptions[] invalidOptions =
        [
            new CsvReaderOptions
            {
                MaxFieldCharacters = CsvReaderOptions.MaximumSupportedFieldCharacters + 1,
            },
            new CsvReaderOptions
            {
                MaxRecordCharacters = CsvReaderOptions.MaximumSupportedRecordCharacters + 1,
            },
            new CsvReaderOptions
            {
                MaxFieldsPerRecord = CsvReaderOptions.MaximumSupportedFieldsPerRecord + 1,
            },
            new CsvReaderOptions
            {
                MaxFieldCharacters = 4,
                NullToken = "oversized",
            },
        ];

        foreach (CsvReaderOptions options in invalidOptions)
        {
            await using var stream = Utf8("id\n1\n");
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await OpenReaderAsync(stream, options));
        }
    }

    [Fact]
    public async Task DetectsUtf8AndUtf16ByteOrderMarksWithStrictDecoding()
    {
        byte[] utf8 = [.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes("name\nÅngström")];
        await using (var stream = new MemoryStream(utf8))
        await using (CsvStreamingReader reader = await OpenReaderAsync(stream))
        {
            CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));
            Assert.True(reader.HasByteOrderMark);
            Assert.Equal("utf-8", reader.ResolvedEncodingName);
            Assert.Equal("Ångström", record.Fields[0].Value);
        }

        var utf16Encoding = new UnicodeEncoding(false, true, true);
        byte[] utf16 = [.. utf16Encoding.GetPreamble(), .. utf16Encoding.GetBytes("name\n東京")];
        await using var utf16Stream = new MemoryStream(utf16);
        await using CsvStreamingReader utf16Reader = await OpenReaderAsync(utf16Stream);
        CsvLogicalRecord utf16Record = Assert.Single(
            await CollectAsync(ReadRecords(utf16Reader)));
        Assert.True(utf16Reader.HasByteOrderMark);
        Assert.Equal("utf-16", utf16Reader.ResolvedEncodingName);
        Assert.Equal("東京", utf16Record.Fields[0].Value);
    }

    [Fact]
    public async Task DetectsBigEndianUtf16AndBothUtf32ByteOrdersAcrossSplitPrefixes()
    {
        Encoding[] encodings =
        [
            new UnicodeEncoding(true, true, true),
            new UTF32Encoding(false, true, true),
            new UTF32Encoding(true, true, true),
        ];

        foreach (Encoding encoding in encodings)
        {
            byte[] bytes = [.. encoding.GetPreamble(), .. encoding.GetBytes("name\n東京")];
            await using var inner = new MemoryStream(bytes);
            await using var stream = new ChunkedReadStream(inner, 1);
            await using CsvStreamingReader reader = await OpenReaderAsync(stream);

            CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

            Assert.True(reader.HasByteOrderMark);
            Assert.Equal(encoding.WebName, reader.ResolvedEncodingName);
            Assert.Equal("東京", record.Fields[0].Value);
        }
    }

    [Fact]
    public async Task RejectsInvalidUtf8InsteadOfReplacingIt()
    {
        byte[] bytes = [.. Encoding.UTF8.GetBytes("name\n"), 0xC3, 0x28];
        await using var stream = new MemoryStream(bytes);
        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () =>
            {
                await using CsvStreamingReader reader = await OpenReaderAsync(stream);
                await CollectAsync(ReadRecords(reader));
            });

        Assert.Equal(CsvDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
        Assert.DoesNotContain("�", exception.ToString(), StringComparison.Ordinal);
        Assert.Null(exception.Diagnostic.LogicalRecordNumber);
        Assert.Null(exception.Diagnostic.DataRecordNumber);
        Assert.Null(exception.Diagnostic.StartPhysicalLine);
        Assert.Null(exception.Diagnostic.EndPhysicalLine);
    }

    [Fact]
    public async Task RejectsInvalidUtf8AfterAByteOrderMark()
    {
        byte[] bytes = [.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes("name\n"), 0xC3, 0x28];
        await using var stream = new MemoryStream(bytes);

        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () =>
            {
                await using CsvStreamingReader reader = await OpenReaderAsync(stream);
                await CollectAsync(ReadRecords(reader));
            });

        Assert.Equal(CsvDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
    }

    [Fact]
    public async Task HonorsLeaveOpenAndSingleEnumerationContract()
    {
        MemoryStream source = Utf8("a\n1");
        var options = new CsvReaderOptions { LeaveOpen = true };
        CsvStreamingReader reader = await OpenReaderAsync(source, options);
        await CollectAsync(ReadRecords(reader));
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await CollectAsync(ReadRecords(reader)));

        await reader.DisposeAsync();
        Assert.True(source.CanRead);
        source.Dispose();
    }

    [Fact]
    public async Task DisposesTheSourceByDefault()
    {
        MemoryStream source = Utf8("a\n1");
        CsvStreamingReader reader = await OpenReaderAsync(source);

        await reader.DisposeAsync();

        Assert.False(source.CanRead);
    }

    [Fact]
    public async Task InvalidConfigurationDoesNotConsumeOrCloseALeaveOpenSource()
    {
        MemoryStream source = Utf8("a\n1");
        var options = new CsvReaderOptions
        {
            Delimiter = " ",
            LeaveOpen = true,
        };

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await OpenReaderAsync(source, options).AsTask());

        Assert.True(source.CanRead);
        Assert.Equal(0, source.Position);
        source.Dispose();
    }

    [Fact]
    public async Task ObservesCancellationBeforeReadingTheNextLogicalRecord()
    {
        await using var stream = Utf8("a\n1");
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await CollectAsync(reader.ReadRecordsAsync(cancellation.Token)));
    }

    [Fact]
    public async Task ObservesCancellationBetweenLogicalRecords()
    {
        await using var stream = Utf8("a\n1\n2");
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        using var cancellation = new CancellationTokenSource();
        await using IAsyncEnumerator<CsvLogicalRecord> enumerator =
            reader.ReadRecordsAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal("1", enumerator.Current.Fields[0].Value);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await enumerator.MoveNextAsync().AsTask());
    }

    [Fact]
    public async Task CancelsAnInFlightLogicalRecordRead()
    {
        await using var stream = new PrefixThenWaitStream(Encoding.UTF8.GetBytes("a\n\"x"));
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        using var cancellation = new CancellationTokenSource();
        await using IAsyncEnumerator<CsvLogicalRecord> enumerator =
            reader.ReadRecordsAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);

        Task<bool> moveNext = enumerator.MoveNextAsync().AsTask();
        await stream.ReadBlocked.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await moveNext.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DoesNotReadTheWholeSourceBeforeYieldingTheFirstRecord()
    {
        string csv = "a,b\n" + string.Concat(Enumerable.Repeat("1,2\n", 300_000));
        await using var inner = Utf8(csv);
        await using var stream = new TrackingReadStream(inner);
        await using CsvStreamingReader reader = await OpenReaderAsync(stream);
        await using IAsyncEnumerator<CsvLogicalRecord> enumerator =
            ReadRecords(reader).GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());

        Assert.Equal("1", enumerator.Current.Fields[0].Value);
        Assert.True(stream.BytesRead < stream.Length / 2);
    }

    [Fact]
    public void PublicSurfaceDoesNotExposeCsvHelperTypes()
    {
        Assembly assembly = typeof(CsvStreamingReader).Assembly;
        Type[] publicTypes = assembly.GetExportedTypes();

        Assert.DoesNotContain(
            publicTypes.SelectMany(GetPublicSignatureTypes),
            type => string.Equals(type.Namespace, "CsvHelper", StringComparison.Ordinal) ||
                    (type.Namespace?.StartsWith("CsvHelper.", StringComparison.Ordinal) ?? false));
    }

    [Fact]
    public async Task RequiresAHeaderRecordWhenConfigured()
    {
        await using var stream = Utf8(string.Empty);
        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await OpenReaderAsync(stream).AsTask());

        Assert.Equal(CsvDiagnosticRules.MissingHeader, exception.Diagnostic.RuleId);
    }

    private static async Task AssertRuleAsync(
        string csv,
        CsvReaderOptions options,
        string expectedRule,
        bool failsWhileOpening = false)
    {
        await using var stream = Utf8(csv);
        if (failsWhileOpening)
        {
            CsvReadException openException = await Assert.ThrowsAsync<CsvReadException>(
                async () => await OpenReaderAsync(stream, options).AsTask());
            Assert.Equal(expectedRule, openException.Diagnostic.RuleId);
            return;
        }

        await using CsvStreamingReader reader = await OpenReaderAsync(stream, options);
        CsvReadException exception = await Assert.ThrowsAsync<CsvReadException>(
            async () => await CollectAsync(ReadRecords(reader)));
        Assert.Equal(expectedRule, exception.Diagnostic.RuleId);
    }

    private static IEnumerable<Type> GetPublicSignatureTypes(Type type)
    {
        yield return type;
        foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
            yield return property.PropertyType;
        foreach (MethodInfo method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
        {
            yield return method.ReturnType;
            foreach (ParameterInfo parameter in method.GetParameters())
                yield return parameter.ParameterType;
        }
    }

    private static MemoryStream Utf8(string value) =>
        new(new UTF8Encoding(false, true).GetBytes(value));

    private static ValueTask<CsvStreamingReader> OpenReaderAsync(
        Stream source,
        CsvReaderOptions? options = null) =>
        CsvStreamingReader.OpenAsync(source, options, TestContext.Current.CancellationToken);

    private static IAsyncEnumerable<CsvLogicalRecord> ReadRecords(CsvStreamingReader reader) =>
        reader.ReadRecordsAsync(TestContext.Current.CancellationToken);

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (T item in source)
            result.Add(item);
        return result;
    }

    private sealed class TrackingReadStream(Stream inner) : Stream
    {
        public long BytesRead { get; private set; }

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = inner.Read(buffer, offset, count);
            BytesRead += read;
            return read;
        }

        public override int Read(Span<byte> buffer)
        {
            int read = inner.Read(buffer);
            BytesRead += read;
            return read;
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            int read = await inner.ReadAsync(buffer, cancellationToken);
            BytesRead += read;
            return read;
        }

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush() => inner.Flush();
        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }

    private sealed class ChunkedReadStream(Stream inner, int maximumReadSize) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            inner.Read(buffer, offset, Math.Min(count, maximumReadSize));

        public override int Read(Span<byte> buffer) =>
            inner.Read(buffer[..Math.Min(buffer.Length, maximumReadSize)]);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            inner.ReadAsync(
                buffer[..Math.Min(buffer.Length, maximumReadSize)],
                cancellationToken);

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }

    private sealed class PrefixThenWaitStream(byte[] prefix) : Stream
    {
        private readonly TaskCompletionSource readBlocked = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private int offset;

        public Task ReadBlocked => readBlocked.Task;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            if (offset < prefix.Length)
            {
                int count = Math.Min(buffer.Length, prefix.Length - offset);
                prefix.AsSpan(offset, count).CopyTo(buffer.Span);
                offset += count;
                return count;
            }

            readBlocked.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
