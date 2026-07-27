using System.Globalization;
using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonStreamingReaderTests
{
    [Fact]
    public async Task RootArrayStreamsEveryTopLevelKindInSourceOrder()
    {
        await using var stream = Utf8(
            " \n[\n null, true, false, \"text\", -0, " +
            "{\"z\":1,\"a\":2}, [3,2,1]\n]\t");
        await using JsonStreamingReader reader = await OpenAsync(stream);

        List<JsonLogicalRecord> records = await CollectAsync(reader.ReadValuesAsync(
            TestContext.Current.CancellationToken));

        Assert.Equal(JsonInputFraming.RootArray, reader.Framing);
        Assert.Equal(JsonInputContracts.EncodingName, reader.ResolvedEncodingName);
        Assert.False(reader.HasByteOrderMark);
        Assert.Equal(7, records.Count);
        Assert.Equal(
            Enumerable.Range(1, records.Count).Select(static value => (long)value),
            records.Select(static record => record.RecordOrdinal));
        Assert.Equal(
            [
                JsonLogicalValueKind.Null,
                JsonLogicalValueKind.Boolean,
                JsonLogicalValueKind.Boolean,
                JsonLogicalValueKind.String,
                JsonLogicalValueKind.Number,
                JsonLogicalValueKind.Object,
                JsonLogicalValueKind.Array,
            ],
            records.Select(static record => record.Value.Kind));
        Assert.True(records[1].Value.BooleanValue);
        Assert.False(records[2].Value.BooleanValue);
        Assert.Equal("text", records[3].Value.StringValue);
        Assert.Equal("-0", records[4].Value.NumberLexeme);
        Assert.Equal(["z", "a"], records[5].Value.Properties.Select(static property => property.Name));
        Assert.Equal(
            ["3", "2", "1"],
            records[6].Value.Elements.Select(static value => value.NumberLexeme));
    }

    [Fact]
    public async Task RootArrayAllowsAnEmptyArray()
    {
        await using var stream = Utf8(" \r\n [ ] \n");
        await using JsonStreamingReader reader = await OpenAsync(stream);

        List<JsonLogicalRecord> records = await CollectAsync(reader.ReadValuesAsync(
            TestContext.Current.CancellationToken));

        Assert.Empty(records);
    }

    [Fact]
    public async Task MultipleValuesAllowsBlankInputBlankLinesAndNoFinalNewline()
    {
        await using var blank = Utf8(" \t\r\n\n");
        await using JsonStreamingReader blankReader = await OpenAsync(
            blank,
            MultipleValues());
        Assert.Empty(await CollectAsync(blankReader.ReadValuesAsync(
            TestContext.Current.CancellationToken)));

        await using var stream = Utf8("\r\n\n null \n\n true\r\n\t{}\t[1,2]");
        await using JsonStreamingReader reader = await OpenAsync(
            stream,
            MultipleValues());

        List<JsonLogicalRecord> records = await CollectAsync(reader.ReadValuesAsync(
            TestContext.Current.CancellationToken));

        Assert.Equal(
            [
                JsonLogicalValueKind.Null,
                JsonLogicalValueKind.Boolean,
                JsonLogicalValueKind.Object,
                JsonLogicalValueKind.Array,
            ],
            records.Select(static record => record.Value.Kind));
        Assert.Equal(["1", "2"], records[3].Value.Elements.Select(static item => item.NumberLexeme));
    }

    [Fact]
    public async Task MultipleValuesRequiresWhitespaceBetweenAdjacentValues()
    {
        JsonReadException exception = await AssertRuleAsync(
            "{}[]",
            MultipleValues(),
            JsonDiagnosticRules.InvalidFraming);

        Assert.Equal(1, exception.Diagnostic.RecordOrdinal);

        await using var valid = Utf8("{} \n []");
        await using JsonStreamingReader reader = await OpenAsync(valid, MultipleValues());
        Assert.Equal(
            2,
            (await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken))).Count);
    }

    [Theory]
    [InlineData("\r")]
    [InlineData("\n")]
    [InlineData("\r\n")]
    public async Task ReportsPhysicalLinesForEveryJsonLineSeparator(
        string separator)
    {
        await using var stream = Utf8($"0{separator}1");
        await using JsonStreamingReader reader = await OpenAsync(
            stream,
            MultipleValues());

        List<JsonLogicalRecord> records = await CollectAsync(
            reader.ReadValuesAsync(TestContext.Current.CancellationToken));

        Assert.Equal(1, records[0].StartLineNumber);
        Assert.Equal(0, records[0].StartBytePositionInLine);
        Assert.Equal(2, records[1].StartLineNumber);
        Assert.Equal(0, records[1].StartBytePositionInLine);
    }

    [Fact]
    public async Task PreservesExactCaseUnicodeNamesExplicitNullAndAbsence()
    {
        await using var stream = Utf8(
            """[{"Name":null,"name":1,"\u0065\u0301":"decomposed","é":"composed"},{}]""");
        await using JsonStreamingReader reader = await OpenAsync(stream);

        List<JsonLogicalRecord> records = await CollectAsync(reader.ReadValuesAsync(
            TestContext.Current.CancellationToken));
        JsonLogicalValue populated = records[0].Value;

        Assert.Equal(["Name", "name", "e\u0301", "é"], populated.Properties.Select(
            static property => property.Name));
        Assert.Equal(
            Enumerable.Range(0, 4),
            populated.Properties.Select(static property => property.Ordinal));
        Assert.Equal(JsonLogicalValueKind.Null, populated.Properties[0].Value.Kind);
        Assert.Equal("1", populated.Properties[1].Value.NumberLexeme);
        Assert.Equal("decomposed", populated.Properties[2].Value.StringValue);
        Assert.Equal("composed", populated.Properties[3].Value.StringValue);
        Assert.Empty(records[1].Value.Properties);
    }

    [Fact]
    public async Task PreservesNestedObjectAndArrayEncounterOrder()
    {
        await using var stream = Utf8(
            """[{"outer":{"second":2,"first":1},"items":[{"y":0,"x":1},2,1]}]""");
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonLogicalValue value = Assert.Single(
            await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken))).Value;

        Assert.Equal(["outer", "items"], value.Properties.Select(static property => property.Name));
        JsonLogicalValue outer = value.Properties[0].Value;
        Assert.Equal(["second", "first"], outer.Properties.Select(static property => property.Name));
        JsonLogicalValue items = value.Properties[1].Value;
        Assert.Equal(
            [JsonLogicalValueKind.Object, JsonLogicalValueKind.Number, JsonLogicalValueKind.Number],
            items.Elements.Select(static element => element.Kind));
        Assert.Equal(
            ["y", "x"],
            items.Elements[0].Properties.Select(static property => property.Name));
        Assert.Equal(
            """{"outer":{"second":2,"first":1},"items":[{"y":0,"x":1},2,1]}"""u8.ToArray(),
            JsonCanonicalValueSerializer.SerializeToUtf8Bytes(
                value,
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RejectsEscapedEquivalentDuplicateAtNestedDepthWithoutLeakingName()
    {
        const string sensitiveName = "super-secret-property";
        JsonReadException exception = await AssertRuleAsync(
            "[{\"outer\":{\"" + sensitiveName +
            "\":1,\"\\u0073uper-secret-property\":2}}]",
            options: null,
            JsonDiagnosticRules.DuplicateProperty);

        Assert.Equal(1, exception.Diagnostic.RecordOrdinal);
        Assert.False(
            exception.Message.Contains(sensitiveName, StringComparison.Ordinal),
            "The deterministic diagnostic must not contain a source property name.");
    }

    [Fact]
    public async Task AllowsCaseDistinctPropertiesAtNestedDepth()
    {
        await using var stream = Utf8("""[{"outer":{"a":1,"A":2}}]""");
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonLogicalValue value = Assert.Single(
            await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken))).Value;

        Assert.Equal(
            ["a", "A"],
            value.Properties[0].Value.Properties.Select(static property => property.Name));
    }

    [Fact]
    public async Task PreservesIntegerDecimalExponentAndNegativeZeroLexemesExactly()
    {
        string[] lexemes =
        [
            "-9223372036854775808",
            "-9223372036854775809",
            "9223372036854775807",
            "9223372036854775808",
            "18446744073709551615",
            "18446744073709551616",
            "12345678901234567890.1234567890123456789000",
            "1e400",
            "1e-4000",
            "-0",
            "1.2300E+004",
            "0.0000",
        ];
        await using var stream = Utf8($"[{string.Join(',', lexemes)}]");
        await using JsonStreamingReader reader = await OpenAsync(stream);

        List<JsonLogicalRecord> records = await CollectAsync(reader.ReadValuesAsync(
            TestContext.Current.CancellationToken));

        Assert.Equal(lexemes, records.Select(static record => record.Value.NumberLexeme));
    }

    [Theory]
    [InlineData("01")]
    [InlineData("-01")]
    [InlineData("-")]
    [InlineData("1.")]
    [InlineData(".1")]
    [InlineData("1e")]
    [InlineData("1e+")]
    [InlineData("--1")]
    [InlineData("+1")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    public async Task RejectsInvalidNumberGrammar(string invalidLexeme)
    {
        await AssertRuleAsync(
            $"[{invalidLexeme}]",
            options: null,
            JsonDiagnosticRules.MalformedData);
    }

    [Fact]
    public async Task AcceptsASplitUtf8BomAndReportsSourceLocationIncludingIt()
    {
        byte[] body = Encoding.UTF8.GetBytes(" \n[\n  {\"x\":1}\n]");
        byte[] bytes = [.. Encoding.UTF8.Preamble, .. body];
        await using var inner = new MemoryStream(bytes);
        await using var stream = new ChunkedReadStream(inner, maximumReadSize: 1);
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonLogicalRecord record = Assert.Single(
            await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));

        Assert.True(reader.HasByteOrderMark);
        Assert.Equal(9, record.StartByteOffset);
        Assert.Equal(16, record.EndByteOffsetExclusive);
        Assert.Equal(7, record.RawByteLength);
        Assert.Equal(3, record.StartLineNumber);
        Assert.Equal(2, record.StartBytePositionInLine);
    }

    [Fact]
    public async Task CountsALeadingUtf8BomInThePhysicalBytePosition()
    {
        byte[] bytes =
        [
            .. Encoding.UTF8.Preamble,
            .. Encoding.UTF8.GetBytes("[0]"),
        ];
        await using var stream = new MemoryStream(bytes);
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonLogicalRecord record = Assert.Single(
            await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));

        Assert.Equal(4, record.StartByteOffset);
        Assert.Equal(1, record.StartLineNumber);
        Assert.Equal(4, record.StartBytePositionInLine);
    }

    public static TheoryData<byte[]> UnsupportedByteOrderMarks => new()
    {
        new byte[] { 0xFF, 0xFE, 0x5B, 0x00 },
        new byte[] { 0xFE, 0xFF, 0x00, 0x5B },
        new byte[] { 0xFF, 0xFE, 0x00, 0x00, 0x5B, 0x00, 0x00, 0x00 },
        new byte[] { 0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, 0x5B },
    };

    [Theory]
    [MemberData(nameof(UnsupportedByteOrderMarks))]
    public async Task RejectsUtf16AndUtf32ByteOrderMarks(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes);

        JsonReadException exception = await Assert.ThrowsAsync<JsonReadException>(
            async () => await OpenAsync(stream).AsTask());

        Assert.Equal(JsonDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
        Assert.Equal(0, exception.Diagnostic.ByteOffset);
    }

    [Fact]
    public async Task RejectsInvalidUtf8WithoutReplacement()
    {
        byte[] bytes =
        [
            (byte)'[',
            (byte)'"',
            0xC3,
            0x28,
            (byte)'"',
            (byte)']',
        ];
        await using var stream = new MemoryStream(bytes);
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonReadException exception = await Assert.ThrowsAsync<JsonReadException>(
            async () => await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));

        Assert.Equal(JsonDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
    }

    [Fact]
    public async Task RejectsInvalidUtf8BeforeRootFraming()
    {
        byte[] bytes = [0xFF, (byte)'[', (byte)']'];
        await using var stream = new MemoryStream(bytes);
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonReadException exception = await Assert.ThrowsAsync<JsonReadException>(
            async () => await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));

        Assert.Equal(JsonDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
        Assert.Equal(0, exception.Diagnostic.ByteOffset);
    }

    [Fact]
    public async Task RejectsATruncatedUtf8SequenceBeforeRootFraming()
    {
        byte[] bytes = [0xF0];
        await using var stream = new MemoryStream(bytes);
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonReadException exception = await Assert.ThrowsAsync<JsonReadException>(
            async () => await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));

        Assert.Equal(JsonDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
        Assert.Equal(0, exception.Diagnostic.ByteOffset);
    }

    [Fact]
    public async Task RejectsATruncatedUtf8SequenceAtEndOfStream()
    {
        byte[] bytes =
        [
            (byte)'"',
            0xF0,
            0x9F,
            0x98,
        ];
        await using var stream = new MemoryStream(bytes);
        await using JsonStreamingReader reader = await OpenAsync(
            stream,
            new JsonStreamingReaderOptions
            {
                Framing = JsonInputFraming.MultipleValues,
            });

        JsonReadException exception = await Assert.ThrowsAsync<JsonReadException>(
            async () => await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));

        Assert.Equal(JsonDiagnosticRules.InvalidEncoding, exception.Diagnostic.RuleId);
        Assert.Equal(1, exception.Diagnostic.ByteOffset);
    }

    [Theory]
    [InlineData("""["\ud800"]""")]
    [InlineData("""["\udc00"]""")]
    [InlineData("""["\ud800\u0041"]""")]
    public async Task RejectsLoneOrMismatchedSurrogateEscapes(string json)
    {
        await AssertRuleAsync(
            json,
            options: null,
            JsonDiagnosticRules.MalformedData);
    }

    [Theory]
    [InlineData("", JsonDiagnosticRules.InvalidFraming)]
    [InlineData("{}", JsonDiagnosticRules.InvalidFraming)]
    [InlineData("[", JsonDiagnosticRules.InvalidFraming)]
    [InlineData("[1", JsonDiagnosticRules.MalformedData)]
    [InlineData("[1,]", JsonDiagnosticRules.MalformedData)]
    [InlineData("[1 2]", JsonDiagnosticRules.InvalidFraming)]
    [InlineData("[/*comment*/1]", JsonDiagnosticRules.MalformedData)]
    [InlineData("[1,//comment\n2]", JsonDiagnosticRules.MalformedData)]
    [InlineData("[1] true", JsonDiagnosticRules.InvalidFraming)]
    [InlineData("[\"unterminated]", JsonDiagnosticRules.MalformedData)]
    [InlineData("""["\q"]""", JsonDiagnosticRules.MalformedData)]
    [InlineData("""["\u12G4"]""", JsonDiagnosticRules.MalformedData)]
    [InlineData("[{\"x\":}]", JsonDiagnosticRules.MalformedData)]
    public async Task RejectsMalformedRootArrayOrFraming(
        string json,
        string expectedRule)
    {
        await AssertRuleAsync(json, options: null, expectedRule);
    }

    [Theory]
    [InlineData("{\"x\":")]
    [InlineData("truefalse")]
    [InlineData("\"unterminated")]
    [InlineData("[1,]")]
    [InlineData("/*comment*/1")]
    public async Task RejectsMalformedMultipleValues(string json)
    {
        await AssertRuleAsync(
            json,
            MultipleValues(),
            JsonDiagnosticRules.MalformedData);
    }

    [Fact]
    public async Task EnforcesValueByteLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxValueBytes = 5,
        };

        JsonLogicalValue exact = await ReadSingleAsync("\"abc\"", options);
        Assert.Equal("abc", exact.StringValue);
        await AssertRuleAsync(
            "\"abcd\"",
            options,
            JsonDiagnosticRules.ValueLimitExceeded);
    }

    [Fact]
    public async Task EnforcesDepthLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with { MaxDepth = 2 };

        Assert.Equal(
            JsonLogicalValueKind.Array,
            (await ReadSingleAsync("[[]]", options)).Kind);
        await AssertRuleAsync(
            "[[[]]]",
            options,
            JsonDiagnosticRules.DepthLimitExceeded);
    }

    [Fact]
    public async Task EnforcesPropertyCountLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxPropertiesPerObject = 2,
        };

        Assert.Equal(
            2,
            (await ReadSingleAsync("""{"a":1,"b":2}""", options)).Properties.Count);
        await AssertRuleAsync(
            """{"a":1,"b":2,"c":3}""",
            options,
            JsonDiagnosticRules.PropertyCountLimitExceeded);
    }

    [Fact]
    public async Task EnforcesArrayElementLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxArrayElements = 2,
        };

        Assert.Equal(2, (await ReadSingleAsync("[0,1]", options)).Elements.Count);
        await AssertRuleAsync(
            "[0,1,2]",
            options,
            JsonDiagnosticRules.ArrayElementLimitExceeded);
    }

    [Fact]
    public async Task EnforcesTotalNodeLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxTotalNodes = 3,
        };

        Assert.Equal(3, (await ReadSingleAsync("[0,1]", options)).NodeCount);
        await AssertRuleAsync(
            "[0,1,2]",
            options,
            JsonDiagnosticRules.NodeCountLimitExceeded);
    }

    [Fact]
    public async Task EnforcesDecodedPropertyNameByteLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxPropertyNameBytes = 2,
        };

        JsonLogicalValue exact = await ReadSingleAsync("""{"\u00e9":0}""", options);
        Assert.Equal("é", Assert.Single(exact.Properties).Name);
        await AssertRuleAsync(
            """{"€":0}""",
            options,
            JsonDiagnosticRules.PropertyNameLimitExceeded);
    }

    [Fact]
    public async Task EnforcesDecodedStringByteLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxStringBytes = 2,
        };

        Assert.Equal(
            "é",
            (await ReadSingleAsync("\"\\u00e9\"", options)).StringValue);
        await AssertRuleAsync(
            "\"€\"",
            options,
            JsonDiagnosticRules.StringLimitExceeded);
    }

    [Fact]
    public async Task EnforcesNumberLexemeByteLimitAtExactAndPlusOne()
    {
        JsonStreamingReaderOptions options = MultipleValues() with
        {
            MaxNumberBytes = 2,
        };

        Assert.Equal("-0", (await ReadSingleAsync("-0", options)).NumberLexeme);
        await AssertRuleAsync(
            "100",
            options,
            JsonDiagnosticRules.NumberLimitExceeded);
    }

    [Fact]
    public async Task RejectsConfiguredLimitsOutsideAbsoluteCeilingsBeforeReading()
    {
        JsonStreamingReaderOptions[] invalidOptions =
        [
            new() { MaxValueBytes = JsonInputContracts.MaximumValueBytes + 1 },
            new() { MaxDepth = JsonInputContracts.MaximumDepth + 1 },
            new()
            {
                MaxPropertiesPerObject =
                    JsonInputContracts.MaximumPropertiesPerObject + 1,
            },
            new()
            {
                MaxArrayElements = JsonInputContracts.MaximumArrayElements + 1,
            },
            new() { MaxTotalNodes = JsonInputContracts.MaximumTotalNodes + 1 },
            new()
            {
                MaxPropertyNameBytes =
                    JsonInputContracts.MaximumPropertyNameBytes + 1,
            },
            new() { MaxStringBytes = JsonInputContracts.MaximumStringBytes + 1 },
            new() { MaxNumberBytes = JsonInputContracts.MaximumNumberBytes + 1 },
        ];

        foreach (JsonStreamingReaderOptions options in invalidOptions)
        {
            await using var stream = new TrackingStream(Encoding.UTF8.GetBytes("[]"));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await OpenAsync(stream, options).AsTask());
            Assert.Equal(0, stream.BytesRead);
        }
    }

    [Fact]
    public async Task ReadsNestedValuesAcrossOneByteNonSeekableChunks()
    {
        byte[] bytes =
        [
            .. Encoding.UTF8.Preamble,
            .. Encoding.UTF8.GetBytes(
                """[{"é":"😀","nested":[true,null,1.2300e+4,{"x":"y"}]}]"""),
        ];
        await using var inner = new MemoryStream(bytes);
        await using var stream = new ChunkedReadStream(inner, maximumReadSize: 1);
        await using JsonStreamingReader reader = await OpenAsync(stream);

        JsonLogicalValue value = Assert.Single(
            await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken))).Value;

        Assert.Equal("é", value.Properties[0].Name);
        Assert.Equal("😀", value.Properties[0].Value.StringValue);
        Assert.Equal(
            "1.2300e+4",
            value.Properties[1].Value.Elements[2].NumberLexeme);
    }

    [Fact]
    public async Task YieldsFirstValueBeforeReadingABlockedSuffix()
    {
        await using var stream = new PrefixThenWaitStream(
            Encoding.UTF8.GetBytes("[{\"id\":1},"));
        await using JsonStreamingReader reader = await OpenAsync(stream);
        await using IAsyncEnumerator<JsonLogicalRecord> enumerator =
            reader.ReadValuesAsync(TestContext.Current.CancellationToken)
                .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        bool available = await enumerator.MoveNextAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.True(available);
        Assert.Equal("1", enumerator.Current.Value.Properties[0].Value.NumberLexeme);
        Assert.False(stream.ReadBlocked.IsCompleted);
    }

    [Fact]
    public async Task YieldsAShortMultipleValueWithoutWaitingForFourPrefixBytes()
    {
        await using var stream = new PrefixThenWaitStream(
            Encoding.UTF8.GetBytes("0\n"));
        await using JsonStreamingReader reader = await OpenAsync(
            stream,
            MultipleValues());
        await using IAsyncEnumerator<JsonLogicalRecord> enumerator =
            reader.ReadValuesAsync(TestContext.Current.CancellationToken)
                .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        bool available = await enumerator.MoveNextAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.True(available);
        Assert.Equal("0", enumerator.Current.Value.NumberLexeme);
        Assert.False(stream.ReadBlocked.IsCompleted);
    }

    [Fact]
    public async Task CancelsAnInFlightReadAfterAYieldedValue()
    {
        await using var stream = new PrefixThenWaitStream(
            Encoding.UTF8.GetBytes("[0, "));
        await using JsonStreamingReader reader = await JsonStreamingReader.OpenAsync(
            stream,
            options: null,
            TestContext.Current.CancellationToken);
        using var cancellation = new CancellationTokenSource();
        await using IAsyncEnumerator<JsonLogicalRecord> enumerator =
            reader.ReadValuesAsync(cancellation.Token)
                .GetAsyncEnumerator(cancellation.Token);
        Assert.True(await enumerator.MoveNextAsync());

        Task<bool> blockedMoveNext = enumerator.MoveNextAsync().AsTask();
        await stream.ReadBlocked.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await blockedMoveNext.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EnforcesSingleEnumeration()
    {
        await using var stream = Utf8("[1]");
        await using JsonStreamingReader reader = await OpenAsync(stream);
        Assert.Single(await CollectAsync(reader.ReadValuesAsync(
            TestContext.Current.CancellationToken)));

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));
    }

    [Fact]
    public async Task HonorsLeaveOpenAndDisposesTheSourceByDefault()
    {
        var owned = new TrackingStream(Encoding.UTF8.GetBytes("[]"));
        JsonStreamingReader ownedReader = await OpenAsync(owned);
        await ownedReader.DisposeAsync();
        Assert.True(owned.IsDisposed);

        var retained = new TrackingStream(Encoding.UTF8.GetBytes("[]"));
        JsonStreamingReader retainedReader = await OpenAsync(
            retained,
            new JsonStreamingReaderOptions { LeaveOpen = true });
        await retainedReader.DisposeAsync();
        Assert.False(retained.IsDisposed);
        await retained.DisposeAsync();
    }

    [Fact]
    public async Task StreamsFiftyThousandValuesWithoutTestSideAccumulation()
    {
        const int count = 50_000;
        var json = new StringBuilder(capacity: count * 6);
        for (int index = 0; index < count; index++)
            json.Append(index).Append('\n');

        await using var stream = Utf8(json.ToString());
        await using JsonStreamingReader reader = await OpenAsync(
            stream,
            MultipleValues());
        int observed = 0;
        await foreach (JsonLogicalRecord record in reader.ReadValuesAsync(
                           TestContext.Current.CancellationToken))
        {
            Assert.Equal(++observed, record.RecordOrdinal);
            Assert.Equal(
                (observed - 1).ToString(CultureInfo.InvariantCulture),
                record.Value.NumberLexeme);
        }

        Assert.Equal(count, observed);
    }

    [Fact]
    public async Task StreamsFiftyThousandRootArrayValuesWithoutTestSideAccumulation()
    {
        const int count = 50_000;
        var json = new StringBuilder(capacity: count * 6);
        json.Append('[');
        for (int index = 0; index < count; index++)
        {
            if (index != 0)
                json.Append(',');
            json.Append(index);
        }
        json.Append(']');

        await using var stream = Utf8(json.ToString());
        await using JsonStreamingReader reader = await OpenAsync(stream);
        int observed = 0;
        await foreach (JsonLogicalRecord record in reader.ReadValuesAsync(
                           TestContext.Current.CancellationToken))
        {
            Assert.Equal(++observed, record.RecordOrdinal);
            Assert.Equal(
                (observed - 1).ToString(CultureInfo.InvariantCulture),
                record.Value.NumberLexeme);
        }

        Assert.Equal(count, observed);
    }

    private static JsonStreamingReaderOptions MultipleValues() => new()
    {
        Framing = JsonInputFraming.MultipleValues,
    };

    private static MemoryStream Utf8(string value) =>
        new(new UTF8Encoding(false, true).GetBytes(value));

    private static ValueTask<JsonStreamingReader> OpenAsync(
        Stream source,
        JsonStreamingReaderOptions? options = null) =>
        JsonStreamingReader.OpenAsync(
            source,
            options,
            TestContext.Current.CancellationToken);

    private static async Task<JsonLogicalValue> ReadSingleAsync(
        string json,
        JsonStreamingReaderOptions options)
    {
        await using var stream = Utf8(json);
        await using JsonStreamingReader reader = await OpenAsync(stream, options);
        return Assert.Single(
            await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken))).Value;
    }

    private static async Task<JsonReadException> AssertRuleAsync(
        string json,
        JsonStreamingReaderOptions? options,
        string expectedRule)
    {
        await using var stream = Utf8(json);
        await using JsonStreamingReader reader = await OpenAsync(stream, options);
        JsonReadException exception = await Assert.ThrowsAsync<JsonReadException>(
            async () => await CollectAsync(reader.ReadValuesAsync(
                TestContext.Current.CancellationToken)));
        Assert.Equal(expectedRule, exception.Diagnostic.RuleId);
        Assert.Equal(JsonDiagnosticRules.Message(expectedRule), exception.Message);
        return exception;
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (T item in source)
            result.Add(item);
        return result;
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

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

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

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }

    private sealed class TrackingStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream inner = new(bytes);

        public bool IsDisposed { get; private set; }

        public long BytesRead { get; private set; }

        public override bool CanRead => !IsDisposed && inner.CanRead;
        public override bool CanSeek => !IsDisposed && inner.CanSeek;
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

        public override long Seek(long offset, SeekOrigin origin) =>
            inner.Seek(offset, origin);

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing && !IsDisposed)
            {
                IsDisposed = true;
                inner.Dispose();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (IsDisposed)
                return;
            IsDisposed = true;
            await inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }
}
