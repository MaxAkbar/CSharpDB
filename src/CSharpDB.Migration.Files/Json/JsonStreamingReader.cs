using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// A single-use, forward-only JSON logical-value reader. The reader retains
/// one explicitly bounded raw value at a time and never buffers the full
/// source.
/// </summary>
public sealed class JsonStreamingReader : IAsyncDisposable
{
    private const int SourceBufferSize = 64 * 1024;
    private const int CancellationCheckInterval = 4 * 1024;

    private readonly Stream source;
    private readonly PrefixedReadStream replaySource;
    private readonly JsonStreamingReaderSettings settings;
    private readonly BufferedByteReader input;
    private int enumerationStarted;
    private bool disposed;

    private JsonStreamingReader(
        Stream source,
        PrefixedReadStream replaySource,
        JsonStreamingReaderSettings settings,
        int preambleLength)
    {
        this.source = source;
        this.replaySource = replaySource;
        this.settings = settings;
        input = new BufferedByteReader(
            replaySource,
            SourceBufferSize,
            initialByteOffset: preambleLength);
        HasByteOrderMark = preambleLength != 0;
    }

    /// <summary>Gets the selected top-level framing mode.</summary>
    public JsonInputFraming Framing => settings.Framing;

    /// <summary>Gets whether one leading UTF-8 byte-order mark was consumed.</summary>
    public bool HasByteOrderMark { get; }

    /// <summary>Gets the frozen strict input encoding name.</summary>
    public string ResolvedEncodingName => JsonInputContracts.EncodingName;

    /// <summary>Opens a strict JSON reader over a readable byte stream.</summary>
    public static async ValueTask<JsonStreamingReader> OpenAsync(
        Stream source,
        JsonStreamingReaderOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!source.CanRead)
        {
            throw new ArgumentException(
                "The JSON source stream must be readable.",
                nameof(source));
        }

        JsonStreamingReaderSettings settings =
            JsonStreamingReaderSettings.Create(
                options ?? new JsonStreamingReaderOptions());
        try
        {
            byte[] prefix = new byte[4];
            int prefixLength = 0;
            while (RequiresMorePreambleBytes(
                       prefix.AsSpan(0, prefixLength)))
            {
                int read = await source.ReadAsync(
                        prefix.AsMemory(
                            prefixLength,
                            prefix.Length - prefixLength),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                prefixLength += read;
            }

            int preambleLength = ResolvePreamble(
                prefix.AsSpan(0, prefixLength));
            byte[] replay = prefix.AsSpan(
                    preambleLength,
                    prefixLength - preambleLength)
                .ToArray();
            var replaySource = new PrefixedReadStream(source, replay);
            return new JsonStreamingReader(
                source,
                replaySource,
                settings,
                preambleLength);
        }
        catch
        {
            if (!settings.LeaveOpen)
                await source.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Streams complete logical values in source order. Enumeration is
    /// single-use.
    /// </summary>
    public async IAsyncEnumerable<JsonLogicalRecord> ReadValuesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (Interlocked.Exchange(ref enumerationStarted, 1) != 0)
        {
            throw new InvalidOperationException(
                "JSON values can be enumerated only once.");
        }

        if (settings.Framing == JsonInputFraming.RootArray)
        {
            await foreach (JsonLogicalRecord record in
                           ReadRootArrayAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                yield return record;
            }
        }
        else
        {
            await foreach (JsonLogicalRecord record in
                           ReadMultipleValuesAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                yield return record;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;

        disposed = true;
        input.Clear();
        await replaySource.DisposeAsync().ConfigureAwait(false);
        if (!settings.LeaveOpen)
            await source.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private async IAsyncEnumerable<JsonLogicalRecord> ReadRootArrayAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
        SourcePosition rootPosition = input.Position;
        int root = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
        if (root != '[')
        {
            await input.CompletePendingUtf8SequenceAsync(cancellationToken)
                .ConfigureAwait(false);
            throw Failure(
                JsonDiagnosticRules.InvalidFraming,
                recordOrdinal: null,
                rootPosition);
        }

        await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
        if (await input.PeekAsync(cancellationToken).ConfigureAwait(false) == ']')
        {
            _ = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
            await RequireWhitespaceThenEndAsync(
                    recordOrdinal: null,
                    cancellationToken)
                .ConfigureAwait(false);
            yield break;
        }

        long recordOrdinal = 0;
        bool followsComma = false;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
            SourcePosition valuePosition = input.Position;
            int next = await input.PeekAsync(cancellationToken).ConfigureAwait(false);
            if (next < 0 || next == ']' || next == ',')
            {
                throw Failure(
                    followsComma
                        ? JsonDiagnosticRules.MalformedData
                        : JsonDiagnosticRules.InvalidFraming,
                    checked(recordOrdinal + 1),
                    valuePosition);
            }

            recordOrdinal = checked(recordOrdinal + 1);
            using FramedValue framed = await ReadFramedValueAsync(
                    recordOrdinal,
                    rootArrayItem: true,
                    cancellationToken)
                .ConfigureAwait(false);
            JsonLogicalValue value = Materialize(
                framed,
                recordOrdinal,
                cancellationToken);
            JsonLogicalRecord record = JsonLogicalRecord.Create(
                recordOrdinal,
                value,
                framed.Start.ByteOffset,
                framed.End.ByteOffset,
                framed.Start.LineNumber,
                framed.Start.BytePositionInLine);

            await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
            SourcePosition separatorPosition = input.Position;
            int separator =
                await input.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (separator == ',')
            {
                followsComma = true;
                yield return record;
                continue;
            }
            if (separator == ']')
            {
                await RequireWhitespaceThenEndAsync(
                        recordOrdinal,
                        cancellationToken)
                    .ConfigureAwait(false);
                yield return record;
                yield break;
            }

            await input.CompletePendingUtf8SequenceAsync(cancellationToken)
                .ConfigureAwait(false);
            throw Failure(
                separator < 0
                    ? JsonDiagnosticRules.MalformedData
                    : JsonDiagnosticRules.InvalidFraming,
                recordOrdinal,
                separatorPosition);
        }
    }

    private async IAsyncEnumerable<JsonLogicalRecord> ReadMultipleValuesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
        long recordOrdinal = 0;
        while (await input.PeekAsync(cancellationToken).ConfigureAwait(false) >= 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            recordOrdinal = checked(recordOrdinal + 1);
            using FramedValue framed = await ReadFramedValueAsync(
                    recordOrdinal,
                    rootArrayItem: false,
                    cancellationToken)
                .ConfigureAwait(false);
            JsonLogicalValue value = Materialize(
                framed,
                recordOrdinal,
                cancellationToken);
            JsonLogicalRecord record = JsonLogicalRecord.Create(
                recordOrdinal,
                value,
                framed.Start.ByteOffset,
                framed.End.ByteOffset,
                framed.Start.LineNumber,
                framed.Start.BytePositionInLine);

            SourcePosition separatorPosition = input.Position;
            int separator = await input.PeekAsync(cancellationToken)
                .ConfigureAwait(false);
            if (separator < 0)
            {
                yield return record;
                yield break;
            }
            if (!IsWhitespace(separator))
            {
                _ = await input.ReadAsync(cancellationToken)
                    .ConfigureAwait(false);
                await input.CompletePendingUtf8SequenceAsync(cancellationToken)
                    .ConfigureAwait(false);
                throw Failure(
                    JsonDiagnosticRules.InvalidFraming,
                    recordOrdinal,
                    separatorPosition);
            }

            _ = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
            yield return record;
            await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<FramedValue> ReadFramedValueAsync(
        long recordOrdinal,
        bool rootArrayItem,
        CancellationToken cancellationToken)
    {
        SourcePosition start = input.Position;
        int first = await input.PeekAsync(cancellationToken).ConfigureAwait(false);
        if (first < 0)
        {
            throw Failure(
                JsonDiagnosticRules.MalformedData,
                recordOrdinal,
                start);
        }

        var buffer = new BoundedValueBuffer(settings.MaxValueBytes);
        try
        {
            if (first is '{' or '[')
            {
                await ReadCompositeAsync(
                        buffer,
                        recordOrdinal,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (first == '"')
            {
                await ReadStringAsync(
                        buffer,
                        recordOrdinal,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await ReadScalarAsync(
                        buffer,
                        recordOrdinal,
                        rootArrayItem,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            if (buffer.Count == 0)
            {
                throw Failure(
                    JsonDiagnosticRules.MalformedData,
                    recordOrdinal,
                    start);
            }

            return new FramedValue(buffer, start, input.Position);
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    private async ValueTask ReadCompositeAsync(
        BoundedValueBuffer buffer,
        long recordOrdinal,
        CancellationToken cancellationToken)
    {
        var closingTokens = new byte[settings.MaxDepth];
        int depth = 0;
        bool inString = false;
        bool escaped = false;
        while (true)
        {
            SourcePosition position = input.Position;
            int next = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (next < 0)
            {
                throw Failure(
                    JsonDiagnosticRules.MalformedData,
                    recordOrdinal,
                    position);
            }

            Append(buffer, checked((byte)next), recordOrdinal, position);
            CheckCancellation(buffer.Count, cancellationToken);
            if (inString)
            {
                if (escaped)
                {
                    escaped = false;
                }
                else if (next == '\\')
                {
                    escaped = true;
                }
                else if (next == '"')
                {
                    inString = false;
                }
                continue;
            }

            if (next == '"')
            {
                inString = true;
                continue;
            }
            if (next is '{' or '[')
            {
                if (depth == settings.MaxDepth)
                {
                    throw Failure(
                        JsonDiagnosticRules.DepthLimitExceeded,
                        recordOrdinal,
                        position,
                        settings.MaxDepth,
                        checked(depth + 1L));
                }

                closingTokens[depth] = next == '{' ? (byte)'}' : (byte)']';
                depth++;
                continue;
            }
            if (next is '}' or ']')
            {
                if (depth == 0 || closingTokens[depth - 1] != next)
                {
                    throw Failure(
                        JsonDiagnosticRules.MalformedData,
                        recordOrdinal,
                        position);
                }

                depth--;
                if (depth == 0)
                    return;
            }
        }
    }

    private async ValueTask ReadStringAsync(
        BoundedValueBuffer buffer,
        long recordOrdinal,
        CancellationToken cancellationToken)
    {
        bool escaped = false;
        bool started = false;
        while (true)
        {
            SourcePosition position = input.Position;
            int next = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (next < 0)
            {
                throw Failure(
                    JsonDiagnosticRules.MalformedData,
                    recordOrdinal,
                    position);
            }

            Append(buffer, checked((byte)next), recordOrdinal, position);
            CheckCancellation(buffer.Count, cancellationToken);
            if (!started)
            {
                started = true;
                continue;
            }
            if (escaped)
            {
                escaped = false;
            }
            else if (next == '\\')
            {
                escaped = true;
            }
            else if (next == '"')
            {
                return;
            }
        }
    }

    private async ValueTask ReadScalarAsync(
        BoundedValueBuffer buffer,
        long recordOrdinal,
        bool rootArrayItem,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            int next = await input.PeekAsync(cancellationToken).ConfigureAwait(false);
            if (next < 0 ||
                IsWhitespace(next) ||
                (rootArrayItem && next is ',' or ']'))
            {
                return;
            }

            SourcePosition position = input.Position;
            _ = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
            Append(buffer, checked((byte)next), recordOrdinal, position);
            CheckCancellation(buffer.Count, cancellationToken);
        }
    }

    private JsonLogicalValue Materialize(
        FramedValue framed,
        long recordOrdinal,
        CancellationToken cancellationToken)
    {
        ReadOnlySpan<byte> raw = framed.Bytes.Span;
        if (raw.StartsWith(Encoding.UTF8.Preamble))
        {
            throw Failure(
                JsonDiagnosticRules.InvalidEncoding,
                recordOrdinal,
                framed.Start);
        }

        var context = new MaterializationContext(
            settings,
            framed,
            recordOrdinal,
            cancellationToken);
        var reader = new Utf8JsonReader(
            raw,
            isFinalBlock: true,
            new System.Text.Json.JsonReaderState(
                new System.Text.Json.JsonReaderOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = settings.MaxDepth,
                }));
        try
        {
            context.ThrowIfCancellationRequested();
            if (!reader.Read())
                throw context.Failure(JsonDiagnosticRules.MalformedData, 0);
            context.ThrowIfCancellationRequested();

            JsonLogicalValue value = ReadLogicalValue(ref reader, context);
            context.ThrowIfCancellationRequested();
            if (reader.Read())
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    reader.TokenStartIndex);
            }
            return value;
        }
        catch (JsonReadException)
        {
            throw;
        }
        catch (JsonException exception)
        {
            throw context.Malformed(exception);
        }
        catch (ArgumentException)
        {
            throw context.Failure(
                JsonDiagnosticRules.MalformedData,
                reader.TokenStartIndex);
        }
        catch (OverflowException)
        {
            throw context.Failure(
                JsonDiagnosticRules.NodeCountLimitExceeded,
                reader.TokenStartIndex,
                settings.MaxTotalNodes,
                checked((long)settings.MaxTotalNodes + 1));
        }
    }

    private static JsonLogicalValue ReadLogicalValue(
        ref Utf8JsonReader reader,
        MaterializationContext context)
    {
        context.ThrowIfCancellationRequested();
        context.CountNode(reader.TokenStartIndex);
        return reader.TokenType switch
        {
            JsonTokenType.Null => JsonLogicalValue.CreateNull(),
            JsonTokenType.True => JsonLogicalValue.CreateBoolean(true),
            JsonTokenType.False => JsonLogicalValue.CreateBoolean(false),
            JsonTokenType.String => ReadStringValue(ref reader, context),
            JsonTokenType.Number => ReadNumberValue(ref reader, context),
            JsonTokenType.StartObject => ReadObject(ref reader, context),
            JsonTokenType.StartArray => ReadArray(ref reader, context),
            _ => throw context.Failure(
                JsonDiagnosticRules.MalformedData,
                reader.TokenStartIndex),
        };
    }

    private static JsonLogicalValue ReadStringValue(
        ref Utf8JsonReader reader,
        MaterializationContext context)
    {
        ReadOnlySpan<byte> token = reader.ValueSpan;
        int decodedBytes = CountDecodedStringBytes(
            token,
            context,
            reader.TokenStartIndex);
        if (decodedBytes > context.Settings.MaxStringBytes)
        {
            throw context.Failure(
                JsonDiagnosticRules.StringLimitExceeded,
                reader.TokenStartIndex,
                context.Settings.MaxStringBytes,
                decodedBytes);
        }

        context.ThrowIfCancellationRequested();
        string value = reader.GetString() ??
            throw context.Failure(
                JsonDiagnosticRules.MalformedData,
                reader.TokenStartIndex);
        context.ThrowIfCancellationRequested();
        return JsonLogicalValue.CreateString(value);
    }

    private static JsonLogicalValue ReadNumberValue(
        ref Utf8JsonReader reader,
        MaterializationContext context)
    {
        ReadOnlySpan<byte> token = reader.ValueSpan;
        if (token.Length > context.Settings.MaxNumberBytes)
        {
            throw context.Failure(
                JsonDiagnosticRules.NumberLimitExceeded,
                reader.TokenStartIndex,
                context.Settings.MaxNumberBytes,
                token.Length);
        }

        context.ThrowIfCancellationRequested();
        string exactLexeme = Encoding.ASCII.GetString(token);
        context.ThrowIfCancellationRequested();
        return JsonLogicalValue.CreateNumber(exactLexeme);
    }

    private static JsonLogicalValue ReadObject(
        ref Utf8JsonReader reader,
        MaterializationContext context)
    {
        var properties = new List<JsonLogicalProperty>();
        var names = new HashSet<string>(StringComparer.Ordinal);
        while (true)
        {
            context.ThrowIfCancellationRequested();
            if (!reader.Read())
                break;
            context.ThrowIfCancellationRequested();
            if (reader.TokenType == JsonTokenType.EndObject)
                return JsonLogicalValue.CreateObject(properties);
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    reader.TokenStartIndex);
            }
            if (properties.Count == context.Settings.MaxPropertiesPerObject)
            {
                throw context.Failure(
                    JsonDiagnosticRules.PropertyCountLimitExceeded,
                    reader.TokenStartIndex,
                    context.Settings.MaxPropertiesPerObject,
                    checked(properties.Count + 1L));
            }

            int decodedBytes = CountDecodedStringBytes(
                reader.ValueSpan,
                context,
                reader.TokenStartIndex);
            if (decodedBytes > context.Settings.MaxPropertyNameBytes)
            {
                throw context.Failure(
                    JsonDiagnosticRules.PropertyNameLimitExceeded,
                    reader.TokenStartIndex,
                    context.Settings.MaxPropertyNameBytes,
                    decodedBytes);
            }

            string name = reader.GetString() ??
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    reader.TokenStartIndex);
            if (!names.Add(name))
            {
                throw context.Failure(
                    JsonDiagnosticRules.DuplicateProperty,
                    reader.TokenStartIndex);
            }
            if (!reader.Read())
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    context.Frame.Bytes.Length);
            }

            JsonLogicalValue value = ReadLogicalValue(ref reader, context);
            properties.Add(
                JsonLogicalProperty.Create(
                    properties.Count,
                    name,
                    value));
        }

        throw context.Failure(
            JsonDiagnosticRules.MalformedData,
            context.Frame.Bytes.Length);
    }

    private static JsonLogicalValue ReadArray(
        ref Utf8JsonReader reader,
        MaterializationContext context)
    {
        var elements = new List<JsonLogicalValue>();
        while (true)
        {
            context.ThrowIfCancellationRequested();
            if (!reader.Read())
                break;
            context.ThrowIfCancellationRequested();
            if (reader.TokenType == JsonTokenType.EndArray)
                return JsonLogicalValue.CreateArray(elements);
            if (elements.Count == context.Settings.MaxArrayElements)
            {
                throw context.Failure(
                    JsonDiagnosticRules.ArrayElementLimitExceeded,
                    reader.TokenStartIndex,
                    context.Settings.MaxArrayElements,
                    checked(elements.Count + 1L));
            }

            elements.Add(ReadLogicalValue(ref reader, context));
        }

        throw context.Failure(
            JsonDiagnosticRules.MalformedData,
            context.Frame.Bytes.Length);
    }

    private static int CountDecodedStringBytes(
        ReadOnlySpan<byte> token,
        MaterializationContext context,
        long tokenStartIndex)
    {
        int decodedBytes = 0;
        int nextCancellationCheck = 0;
        long valueStartIndex = checked(tokenStartIndex + 1);
        for (int index = 0; index < token.Length;)
        {
            if (index >= nextCancellationCheck)
            {
                context.ThrowIfCancellationRequested();
                nextCancellationCheck = checked(index + CancellationCheckInterval);
            }

            byte current = token[index++];
            if (current != '\\')
            {
                decodedBytes = checked(decodedBytes + 1);
                continue;
            }
            if (index >= token.Length)
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    checked(valueStartIndex + index - 1));
            }

            byte escape = token[index++];
            if (escape is 0x22 or 0x5C or 0x2F or 0x62 or
                0x66 or 0x6E or 0x72 or 0x74)
            {
                decodedBytes = checked(decodedBytes + 1);
                continue;
            }
            if (escape != 'u' || index + 4 > token.Length)
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    checked(valueStartIndex + index - 2));
            }

            int scalar = ReadHexScalar(
                token.Slice(index, 4),
                context,
                checked(valueStartIndex + index));
            index += 4;
            if (scalar is >= 0xD800 and <= 0xDBFF)
            {
                if (index + 6 > token.Length ||
                    token[index] != '\\' ||
                    token[index + 1] != 'u')
                {
                    throw context.Failure(
                        JsonDiagnosticRules.MalformedData,
                        checked(valueStartIndex + index - 6));
                }

                int low = ReadHexScalar(
                    token.Slice(index + 2, 4),
                    context,
                    checked(valueStartIndex + index + 2));
                if (low is < 0xDC00 or > 0xDFFF)
                {
                    throw context.Failure(
                        JsonDiagnosticRules.MalformedData,
                        checked(valueStartIndex + index));
                }
                index += 6;
                decodedBytes = checked(decodedBytes + 4);
            }
            else if (scalar is >= 0xDC00 and <= 0xDFFF)
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    checked(valueStartIndex + index - 6));
            }
            else
            {
                decodedBytes = checked(
                    decodedBytes +
                    (scalar <= 0x7F
                        ? 1
                        : scalar <= 0x7FF
                            ? 2
                            : 3));
            }
        }

        context.ThrowIfCancellationRequested();
        return decodedBytes;
    }

    private static int ReadHexScalar(
        ReadOnlySpan<byte> digits,
        MaterializationContext context,
        long relativeOffset)
    {
        int value = 0;
        for (int index = 0; index < digits.Length; index++)
        {
            byte digit = digits[index];
            int hex = digit switch
            {
                >= (byte)'0' and <= (byte)'9' => digit - '0',
                >= (byte)'a' and <= (byte)'f' => digit - 'a' + 10,
                >= (byte)'A' and <= (byte)'F' => digit - 'A' + 10,
                _ => -1,
            };
            if (hex < 0)
            {
                throw context.Failure(
                    JsonDiagnosticRules.MalformedData,
                    checked(relativeOffset + index));
            }
            value = (value << 4) | hex;
        }
        return value;
    }

    private async ValueTask ConsumeWhitespaceAsync(
        CancellationToken cancellationToken)
    {
        while (true)
        {
            int next = await input.PeekAsync(cancellationToken)
                .ConfigureAwait(false);
            if (!IsWhitespace(next))
                return;
            _ = await input.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask RequireWhitespaceThenEndAsync(
        long? recordOrdinal,
        CancellationToken cancellationToken)
    {
        await ConsumeWhitespaceAsync(cancellationToken).ConfigureAwait(false);
        SourcePosition position = input.Position;
        if (await input.PeekAsync(cancellationToken).ConfigureAwait(false) >= 0)
        {
            _ = await input.ReadAsync(cancellationToken)
                .ConfigureAwait(false);
            await input.CompletePendingUtf8SequenceAsync(cancellationToken)
                .ConfigureAwait(false);
            throw Failure(
                JsonDiagnosticRules.InvalidFraming,
                recordOrdinal,
                position);
        }
    }

    private void Append(
        BoundedValueBuffer buffer,
        byte value,
        long recordOrdinal,
        SourcePosition position)
    {
        if (buffer.Count == settings.MaxValueBytes)
        {
            throw Failure(
                JsonDiagnosticRules.ValueLimitExceeded,
                recordOrdinal,
                position,
                settings.MaxValueBytes,
                checked(buffer.Count + 1L));
        }
        buffer.Append(value);
    }

    private static void CheckCancellation(
        int byteCount,
        CancellationToken cancellationToken)
    {
        if ((byteCount & (CancellationCheckInterval - 1)) == 0)
            cancellationToken.ThrowIfCancellationRequested();
    }

    private JsonReadException Failure(
        string ruleId,
        long? recordOrdinal,
        SourcePosition position,
        long? limit = null,
        long? observed = null) =>
        new(
            JsonReadDiagnostic.Create(
                ruleId,
                recordOrdinal,
                position.ByteOffset,
                position.LineNumber,
                position.BytePositionInLine,
                limit,
                observed));

    private static int ResolvePreamble(ReadOnlySpan<byte> prefix)
    {
        if (prefix.StartsWith(new byte[] { 0x00, 0x00, 0xFE, 0xFF }) ||
            prefix.StartsWith(new byte[] { 0xFF, 0xFE, 0x00, 0x00 }) ||
            prefix.StartsWith(new byte[] { 0xFE, 0xFF }) ||
            prefix.StartsWith(new byte[] { 0xFF, 0xFE }))
        {
            throw new JsonReadException(
                JsonReadDiagnostic.Create(
                    JsonDiagnosticRules.InvalidEncoding,
                    byteOffset: 0,
                    lineNumber: 1,
                    bytePositionInLine: 0));
        }
        return prefix.StartsWith(Encoding.UTF8.Preamble) ? 3 : 0;
    }

    private static bool RequiresMorePreambleBytes(
        ReadOnlySpan<byte> prefix) =>
        prefix.Length switch
        {
            0 => true,
            1 => prefix[0] is 0x00 or 0xEF or 0xFE or 0xFF,
            2 => prefix is [0x00, 0x00] or [0xEF, 0xBB],
            3 => prefix is [0x00, 0x00, 0xFE],
            _ => false,
        };

    private static bool IsWhitespace(int value) =>
        value is ' ' or '\t' or '\r' or '\n';

    private sealed class MaterializationContext(
        JsonStreamingReaderSettings settings,
        FramedValue frame,
        long recordOrdinal,
        CancellationToken cancellationToken)
    {
        private int nodeCount;

        internal JsonStreamingReaderSettings Settings { get; } = settings;

        internal FramedValue Frame { get; } = frame;

        internal void ThrowIfCancellationRequested() =>
            cancellationToken.ThrowIfCancellationRequested();

        internal void CountNode(long relativeOffset)
        {
            ThrowIfCancellationRequested();
            if (nodeCount == Settings.MaxTotalNodes)
            {
                throw Failure(
                    JsonDiagnosticRules.NodeCountLimitExceeded,
                    relativeOffset,
                    Settings.MaxTotalNodes,
                    checked(nodeCount + 1L));
            }
            nodeCount++;
        }

        internal JsonReadException Failure(
            string ruleId,
            long relativeOffset,
            long? limit = null,
            long? observed = null)
        {
            SourcePosition position = PositionAt(relativeOffset);
            return new JsonReadException(
                JsonReadDiagnostic.Create(
                    ruleId,
                    recordOrdinal,
                    position.ByteOffset,
                    position.LineNumber,
                    position.BytePositionInLine,
                    limit,
                    observed));
        }

        internal JsonReadException Malformed(JsonException exception)
        {
            long relativeLine = exception.LineNumber.GetValueOrDefault();
            long relativeByte =
                exception.BytePositionInLine.GetValueOrDefault();
            SourcePosition position =
                PositionAtLine(relativeLine, relativeByte);
            return new JsonReadException(
                JsonReadDiagnostic.Create(
                    JsonDiagnosticRules.MalformedData,
                    recordOrdinal,
                    position.ByteOffset,
                    position.LineNumber,
                    position.BytePositionInLine));
        }

        private SourcePosition PositionAt(long relativeOffset)
        {
            int bounded = checked((int)Math.Clamp(
                relativeOffset,
                0,
                Frame.Bytes.Length));
            long line = Frame.Start.LineNumber;
            long byteInLine = Frame.Start.BytePositionInLine;
            bool previousWasCarriageReturn = false;
            ReadOnlySpan<byte> bytes = Frame.Bytes.Span;
            for (int index = 0; index < bounded; index++)
            {
                if (bytes[index] == '\r')
                {
                    line++;
                    byteInLine = 0;
                    previousWasCarriageReturn = true;
                }
                else if (bytes[index] == '\n')
                {
                    if (!previousWasCarriageReturn)
                        line++;
                    byteInLine = 0;
                    previousWasCarriageReturn = false;
                }
                else
                {
                    byteInLine++;
                    previousWasCarriageReturn = false;
                }
            }

            return new SourcePosition(
                checked(Frame.Start.ByteOffset + bounded),
                line,
                byteInLine);
        }

        private SourcePosition PositionAtLine(
            long relativeLine,
            long relativeBytePosition)
        {
            long currentLine = 0;
            long currentByte = 0;
            ReadOnlySpan<byte> bytes = Frame.Bytes.Span;
            int index = 0;
            while (index < bytes.Length)
            {
                if (currentLine == relativeLine &&
                    currentByte == relativeBytePosition)
                {
                    break;
                }

                if (bytes[index++] == '\n')
                {
                    currentLine++;
                    currentByte = 0;
                }
                else
                {
                    currentByte++;
                }
            }
            return PositionAt(index);
        }
    }

    private sealed class FramedValue(
        BoundedValueBuffer buffer,
        SourcePosition start,
        SourcePosition end) : IDisposable
    {
        internal ReadOnlyMemory<byte> Bytes => buffer.WrittenMemory;

        internal SourcePosition Start { get; } = start;

        internal SourcePosition End { get; } = end;

        public void Dispose() => buffer.Dispose();
    }

    private sealed class BoundedValueBuffer : IDisposable
    {
        private readonly int maximum;
        private byte[] bytes;
        private bool disposed;

        internal BoundedValueBuffer(int maximum)
        {
            this.maximum = maximum;
            bytes = new byte[Math.Min(maximum, 256)];
        }

        internal int Count { get; private set; }

        internal ReadOnlyMemory<byte> WrittenMemory =>
            bytes.AsMemory(0, Count);

        internal void Append(byte value)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (Count == bytes.Length)
            {
                int nextLength = Math.Min(
                    maximum,
                    checked(Math.Max(Count + 1, bytes.Length * 2)));
                Array.Resize(ref bytes, nextLength);
            }
            bytes[Count++] = value;
        }

        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;
            CryptographicOperations.ZeroMemory(bytes);
            bytes = [];
            Count = 0;
        }
    }

    private sealed class BufferedByteReader(
        Stream source,
        int bufferSize,
        long initialByteOffset)
    {
        private readonly byte[] buffer = new byte[bufferSize];
        private readonly StrictUtf8Validator utf8 = new();
        private int offset;
        private int count;
        private bool previousWasCarriageReturn;

        internal SourcePosition Position { get; private set; } =
            new(initialByteOffset, 1, initialByteOffset);

        internal async ValueTask<int> PeekAsync(
            CancellationToken cancellationToken)
        {
            if (offset == count)
            {
                offset = 0;
                count = await source.ReadAsync(buffer, cancellationToken)
                    .ConfigureAwait(false);
                if (count == 0)
                {
                    utf8.RequireComplete();
                    return -1;
                }
            }
            utf8.Validate(buffer[offset], Position, advance: false);
            return buffer[offset];
        }

        internal async ValueTask<int> ReadAsync(
            CancellationToken cancellationToken)
        {
            int value = await PeekAsync(cancellationToken).ConfigureAwait(false);
            if (value < 0)
                return value;

            utf8.Validate(checked((byte)value), Position, advance: true);
            offset++;
            long nextOffset = checked(Position.ByteOffset + 1);
            if (value == '\r')
            {
                Position = new SourcePosition(
                    nextOffset,
                    checked(Position.LineNumber + 1),
                    0);
                previousWasCarriageReturn = true;
            }
            else if (value == '\n')
            {
                Position = new SourcePosition(
                    nextOffset,
                    previousWasCarriageReturn
                        ? Position.LineNumber
                        : checked(Position.LineNumber + 1),
                    0);
                previousWasCarriageReturn = false;
            }
            else
            {
                Position = new SourcePosition(
                    nextOffset,
                    Position.LineNumber,
                    checked(Position.BytePositionInLine + 1));
                previousWasCarriageReturn = false;
            }
            return value;
        }

        internal async ValueTask CompletePendingUtf8SequenceAsync(
            CancellationToken cancellationToken)
        {
            while (utf8.HasPendingSequence)
            {
                _ = await ReadAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal void Clear() =>
            CryptographicOperations.ZeroMemory(buffer);
    }

    private sealed class StrictUtf8Validator
    {
        private int remaining;
        private byte nextMinimum = 0x80;
        private byte nextMaximum = 0xBF;
        private SourcePosition sequenceStart;

        internal bool HasPendingSequence => remaining != 0;

        internal void Validate(
            byte value,
            SourcePosition position,
            bool advance)
        {
            int nextRemaining = remaining;
            byte minimum = nextMinimum;
            byte maximum = nextMaximum;
            SourcePosition start = sequenceStart;
            if (nextRemaining == 0)
            {
                if (value <= 0x7F)
                    return;

                start = position;
                switch (value)
                {
                    case >= 0xC2 and <= 0xDF:
                        nextRemaining = 1;
                        minimum = 0x80;
                        maximum = 0xBF;
                        break;
                    case 0xE0:
                        nextRemaining = 2;
                        minimum = 0xA0;
                        maximum = 0xBF;
                        break;
                    case >= 0xE1 and <= 0xEC:
                    case >= 0xEE and <= 0xEF:
                        nextRemaining = 2;
                        minimum = 0x80;
                        maximum = 0xBF;
                        break;
                    case 0xED:
                        nextRemaining = 2;
                        minimum = 0x80;
                        maximum = 0x9F;
                        break;
                    case 0xF0:
                        nextRemaining = 3;
                        minimum = 0x90;
                        maximum = 0xBF;
                        break;
                    case >= 0xF1 and <= 0xF3:
                        nextRemaining = 3;
                        minimum = 0x80;
                        maximum = 0xBF;
                        break;
                    case 0xF4:
                        nextRemaining = 3;
                        minimum = 0x80;
                        maximum = 0x8F;
                        break;
                    default:
                        throw InvalidEncoding(position);
                }
            }
            else
            {
                if (value < minimum || value > maximum)
                    throw InvalidEncoding(position);

                nextRemaining--;
                minimum = 0x80;
                maximum = 0xBF;
            }

            if (!advance)
                return;
            remaining = nextRemaining;
            nextMinimum = minimum;
            nextMaximum = maximum;
            sequenceStart = start;
        }

        internal void RequireComplete()
        {
            if (remaining != 0)
                throw InvalidEncoding(sequenceStart);
        }

        private static JsonReadException InvalidEncoding(
            SourcePosition position) =>
            new(
                JsonReadDiagnostic.Create(
                    JsonDiagnosticRules.InvalidEncoding,
                    byteOffset: position.ByteOffset,
                    lineNumber: position.LineNumber,
                    bytePositionInLine: position.BytePositionInLine));
    }

    private readonly record struct SourcePosition(
        long ByteOffset,
        long LineNumber,
        long BytePositionInLine);
}
