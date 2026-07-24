using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// One physical source row supplied in strictly increasing row-ID order.
/// The row ID establishes order only and is not written or hashed.
/// </summary>
public readonly record struct JsonExportRow(
    long RowId,
    ReadOnlyMemory<DbValue> Values);

/// <summary>
/// Complete restart-only input for one deterministic streaming JSON export.
/// </summary>
public sealed record JsonStreamingExportRequest
{
    public required JsonExportProfile Profile { get; init; }

    public required JsonExportFraming Framing { get; init; }

    public required JsonExportSourceManifest Source { get; init; }

    public required TableSchema Table { get; init; }

    public required IAsyncEnumerable<JsonExportRow> Rows { get; init; }

    /// <summary>Maximum number of JSON data bytes that may be emitted.</summary>
    public long MaxDataBytes { get; init; } = 1L << 40;

    /// <summary>
    /// Per-value decoded BLOB ceiling recorded in every BLOB column contract.
    /// </summary>
    public int MaximumDecodedBlobBytes { get; init; } =
        JsonExportContracts.MaximumSupportedDecodedBlobBytes;
}

/// <summary>
/// Successful JSON output evidence. A failed export returns no result; any
/// partial destination bytes remain caller-owned and must be restarted at zero.
/// </summary>
public sealed record JsonStreamingExportResult
{
    public required JsonExportManifest Manifest { get; init; }

    public required byte[] CanonicalManifestBytes { get; init; }

    public required string ManifestDigest { get; init; }
}

/// <summary>
/// Writes deterministic compact JSON or NDJSON to a caller-owned empty stream.
/// The exporter flushes but does not claim durable storage.
/// </summary>
public sealed partial class JsonStreamingExporter
{
    private const int Utf8BufferBytes = 16 * 1024;
    private const int Utf8InputChunkCharacters = 4 * 1024;
    private const int BlobInputChunkBytes = 12 * 1024;

    private static readonly UTF8Encoding s_strictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    private static readonly ReadOnlyMemory<byte> s_arrayStart =
        new byte[] { (byte)'[' };

    private static readonly ReadOnlyMemory<byte> s_arrayEndNewline =
        new byte[] { (byte)']', (byte)'\n' };

    private static readonly ReadOnlyMemory<byte> s_objectStart =
        new byte[] { (byte)'{' };

    private static readonly ReadOnlyMemory<byte> s_objectEnd =
        new byte[] { (byte)'}' };

    private static readonly ReadOnlyMemory<byte> s_comma =
        new byte[] { (byte)',' };

    private static readonly ReadOnlyMemory<byte> s_colon =
        new byte[] { (byte)':' };

    private static readonly ReadOnlyMemory<byte> s_quote =
        new byte[] { (byte)'"' };

    private static readonly ReadOnlyMemory<byte> s_escapedQuote =
        new byte[] { (byte)'\\', (byte)'"' };

    private static readonly ReadOnlyMemory<byte> s_escapedBackslash =
        new byte[] { (byte)'\\', (byte)'\\' };

    private static readonly ReadOnlyMemory<byte> s_newline =
        new byte[] { (byte)'\n' };

    private static readonly ReadOnlyMemory<byte> s_null =
        "null"u8.ToArray();

    private static readonly ReadOnlyMemory<byte>[] s_controlEscapes =
        CreateControlEscapes();

    public async ValueTask<JsonStreamingExportResult> WriteAsync(
        Stream destination,
        JsonStreamingExportRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(request);

        ValidateRestartOnlyDestination(destination);
        cancellationToken.ThrowIfCancellationRequested();
        PreparedRequest prepared =
            PrepareRequest(request, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        using var sink =
            new ExportByteSink(
                destination,
                request.MaxDataBytes);
        using var sourceDigest =
            new JsonExportOrderedContentDigest();
        using var exportedDigest =
            new JsonExportOrderedContentDigest();

        await WriteFramingStartAsync(
                sink,
                prepared.Framing,
                cancellationToken)
            .ConfigureAwait(false);

        long rowCount = 0;
        bool hasPreviousRowId = false;
        long previousRowId = 0;

        await foreach (
            JsonExportRow row in
            request.Rows
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (hasPreviousRowId &&
                row.RowId <= previousRowId)
            {
                throw new InvalidDataException(
                    "JSON export rows must have strictly increasing physical row IDs.");
            }
            if (rowCount == long.MaxValue)
            {
                throw new OverflowException(
                    "JSON export row count exceeds the signed 64-bit contract.");
            }

            bool followsRow = rowCount != 0;
            PreparedRow preparedRow =
                await PrepareRowAsync(
                        row,
                        prepared,
                        sink.BytesWritten,
                        followsRow,
                        request.MaxDataBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
            try
            {
                await WriteVerifiedRowAsync(
                        sink,
                        prepared,
                        preparedRow,
                        followsRow,
                        cancellationToken)
                    .ConfigureAwait(false);

                sourceDigest.AppendRowHash(
                    preparedRow
                        .SourceCanonicalRowHash);
                exportedDigest.AppendRowHash(
                    preparedRow
                        .ExportedCanonicalRowHash);
                rowCount++;
                previousRowId = row.RowId;
                hasPreviousRowId = true;
            }
            finally
            {
                preparedRow.ClearSensitiveBuffers();
            }
        }

        await WriteFramingEndAsync(
                sink,
                prepared.Framing,
                cancellationToken)
            .ConfigureAwait(false);

        await destination
            .FlushAsync(cancellationToken)
            .ConfigureAwait(false);

        JsonExportHashManifest sourceLogicalDigest =
            sourceDigest.Complete();
        JsonExportHashManifest exportedLogicalDigest =
            exportedDigest.Complete();
        JsonExportHashManifest dataDigest =
            sink.CompleteHash();

        if (sourceDigest.RowCount != rowCount ||
            exportedDigest.RowCount != rowCount)
        {
            throw new InvalidOperationException(
                "JSON export logical row counts diverged.");
        }
        if (!HashEquals(
                sourceLogicalDigest,
                exportedLogicalDigest))
        {
            throw new InvalidOperationException(
                "Lossless JSON source and exported logical digests diverged.");
        }

        JsonExportManifest manifest = CreateManifest(
            prepared,
            rowCount,
            sink.BytesWritten,
            dataDigest,
            sourceLogicalDigest,
            exportedLogicalDigest);
        byte[] canonicalManifestBytes =
            JsonExportManifestSerializer.Serialize(
                manifest);
        try
        {
            string manifestDigest =
                JsonExportManifestSerializer
                    .ComputeManifestDigest(manifest);
            return new JsonStreamingExportResult
            {
                Manifest = manifest,
                CanonicalManifestBytes =
                    canonicalManifestBytes,
                ManifestDigest = manifestDigest,
            };
        }
        catch
        {
            CryptographicOperations.ZeroMemory(
                canonicalManifestBytes);
            throw;
        }
    }

    private static async ValueTask
        WriteFramingStartAsync(
        ExportByteSink sink,
        JsonExportFraming framing,
        CancellationToken cancellationToken)
    {
        switch (framing)
        {
            case JsonExportFraming.RootArray:
                // Reserve the opening byte and exact two-byte closing tail
                // before exposing any partial root-array output.
                sink.EnsureCanWrite(3);
                await sink
                    .WriteAsync(
                        s_arrayStart,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case JsonExportFraming.Ndjson:
                break;

            default:
                throw new InvalidOperationException(
                    "Prepared JSON export framing is unsupported.");
        }
    }

    private static async ValueTask
        WriteFramingEndAsync(
        ExportByteSink sink,
        JsonExportFraming framing,
        CancellationToken cancellationToken)
    {
        switch (framing)
        {
            case JsonExportFraming.RootArray:
                await sink
                    .WriteAsync(
                        s_arrayEndNewline,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case JsonExportFraming.Ndjson:
                break;

            default:
                throw new InvalidOperationException(
                    "Prepared JSON export framing is unsupported.");
        }
    }

    private static PreparedRequest PrepareRequest(
        JsonStreamingExportRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Rows is null)
        {
            throw new ArgumentException(
                "JSON export rows are required.",
                nameof(request));
        }
        if (request.Table is null)
        {
            throw new ArgumentException(
                "JSON export table schema is required.",
                nameof(request));
        }
        if (request.Source is null)
        {
            throw new ArgumentException(
                "JSON export source evidence is required.",
                nameof(request));
        }
        if (!Enum.IsDefined(request.Profile))
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "JSON export profile is unsupported.");
        }
        if (request.Profile !=
            JsonExportProfile.LosslessV1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "JSON export v1 supports only its lossless profile.");
        }
        if (!Enum.IsDefined(request.Framing))
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "JSON export framing is unsupported.");
        }
        if (request.MaxDataBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "JSON export byte ceiling must be positive.");
        }
        if (request.MaximumDecodedBlobBytes is
            < 1 or >
            JsonExportContracts
                .MaximumSupportedDecodedBlobBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "JSON export decoded BLOB ceiling is outside the supported range.");
        }
        if (request.Framing ==
                JsonExportFraming.RootArray &&
            request.MaxDataBytes < 3)
        {
            throw new InvalidDataException(
                "A root-array JSON export requires at least three data bytes.");
        }

        string tableName =
            request.Table.TableName ??
            throw new ArgumentException(
                "JSON export table name is required.",
                nameof(request));
        ValidateManifestText(
            tableName,
            "JSON export table name");

        IReadOnlyList<ColumnDefinition> sourceColumns =
            request.Table.Columns ??
            throw new ArgumentException(
                "JSON export columns are required.",
                nameof(request));
        if (sourceColumns.Count is
            < 1 or >
            JsonInputContracts
                .MaximumPropertiesPerObject)
        {
            throw new InvalidDataException(
                $"JSON export column count must be between 1 and {JsonInputContracts.MaximumPropertiesPerObject}.");
        }

        var bindings =
            new ColumnBinding[sourceColumns.Count];
        var manifestColumns =
            new JsonExportColumnManifest[
                sourceColumns.Count];
        var propertyNames =
            new HashSet<string>(
                StringComparer.Ordinal);
        long minimumObjectByteLength =
            checked(2L + sourceColumns.Count - 1L);

        for (int index = 0;
             index < sourceColumns.Count;
             index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ColumnDefinition column =
                sourceColumns[index] ??
                throw new InvalidDataException(
                    $"JSON export schema column {index} is missing.");
            string sourceName =
                column.Name ??
                throw new InvalidDataException(
                    $"JSON export schema column {index} has no name.");
            ValidateManifestText(
                sourceName,
                $"JSON export property {index}");
            if (!propertyNames.Add(sourceName))
            {
                throw new InvalidDataException(
                    "JSON export property names must be unique under ordinal comparison.");
            }

            JsonStringMetrics nameMetrics =
                MeasureJsonString(
                    sourceName,
                    JsonInputContracts
                        .MaximumPropertyNameBytes,
                    $"JSON export property {index}",
                    cancellationToken);
            long propertySyntaxByteLength =
                checked(
                    nameMetrics
                        .JsonLiteralByteLength +
                    1L);
            minimumObjectByteLength =
                checked(
                    minimumObjectByteLength +
                    propertySyntaxByteLength);

            (
                JsonExportDatabaseType databaseType,
                CanonicalType canonicalType,
                string valueEncoding) =
                MapType(column.Type);
            int maximumDecodedBytes =
                databaseType ==
                JsonExportDatabaseType.Blob
                    ? request
                        .MaximumDecodedBlobBytes
                    : 0;

            bindings[index] = new ColumnBinding(
                index,
                sourceName,
                column.Type,
                canonicalType,
                column.Nullable,
                maximumDecodedBytes,
                propertySyntaxByteLength);
            manifestColumns[index] =
                new JsonExportColumnManifest
                {
                    Ordinal = index,
                    SourceName = sourceName,
                    PropertyName = sourceName,
                    DatabaseType = databaseType,
                    Nullable = column.Nullable,
                    ValueEncoding = valueEncoding,
                    MaximumDecodedBytes =
                        maximumDecodedBytes,
                };
        }

        if (minimumObjectByteLength >
            JsonInputContracts.MaximumValueBytes)
        {
            throw new InvalidDataException(
                "JSON export object property syntax exceeds the strict reader's logical-value byte ceiling.");
        }

        JsonExportHashManifest schemaDigest =
            JsonExportManifestSerializer
                .ComputeSchemaDigest(
                    manifestColumns);
        var table =
            new JsonExportTableManifest
            {
                Name = tableName,
                SchemaContract =
                    JsonExportContracts.Schema,
                SchemaDigest = schemaDigest,
                RowOrder =
                    JsonExportContracts.RowOrder,
                Columns =
                    Array.AsReadOnly(
                        manifestColumns),
            };
        JsonExportSourceManifest source =
            CopySource(request.Source);
        JsonExportFormatManifest format =
            CreateFormat(request);
        var prepared = new PreparedRequest(
            request.Profile,
            request.Framing,
            source,
            table,
            format,
            bindings,
            minimumObjectByteLength);

        JsonExportManifest provisional =
            CreateProvisionalManifest(prepared);
        byte[] provisionalBytes =
            JsonExportManifestSerializer.Serialize(
                provisional);
        CryptographicOperations.ZeroMemory(
            provisionalBytes);
        return prepared;
    }

    private static async ValueTask<PreparedRow>
        PrepareRowAsync(
        JsonExportRow row,
        PreparedRequest request,
        long bytesAlreadyWritten,
        bool followsRow,
        long maximumDataBytes,
        CancellationToken cancellationToken)
    {
        if (row.Values.Length !=
            request.Bindings.Length)
        {
            throw new InvalidDataException(
                $"JSON export row {row.RowId} has {row.Values.Length} fields; {request.Bindings.Length} were required.");
        }

        var values =
            new PreparedValue[
                request.Bindings.Length];
        var sourceLogicalValues =
            new CanonicalValue[
                request.Bindings.Length];
        byte[]? sourceCanonicalRowHash = null;
        byte[]? exportedCanonicalRowHash = null;
        byte[]? renderedObject = null;
        long objectByteLength =
            request.MinimumObjectByteLength;
        DbValue[] sourceValues =
            row.Values.ToArray();

        try
        {
            for (int index = 0;
                 index < request.Bindings.Length;
                 index++)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                ColumnBinding binding =
                    request.Bindings[index];
                DbValue value = sourceValues[index];
                if (value.IsNull)
                {
                    if (!binding.Nullable)
                    {
                        throw new InvalidDataException(
                            $"JSON export row {row.RowId}, column '{binding.PropertyName}' is null but the source schema marks it non-nullable.");
                    }

                    values[index] =
                        PreparedValue.Null;
                    CanonicalValue canonical =
                        CanonicalValue.Null(
                            binding.CanonicalType);
                    sourceLogicalValues[index] =
                        canonical;
                    objectByteLength =
                        checked(
                            objectByteLength + 4L);
                    continue;
                }

                if (value.Type !=
                    binding.SourceType)
                {
                    throw new InvalidDataException(
                        $"JSON export row {row.RowId}, column '{binding.PropertyName}' has runtime type {value.Type}; source type {binding.SourceType} was required.");
                }

                switch (binding.SourceType)
                {
                    case DbType.Integer:
                        {
                            long scalar =
                                value.AsInteger;
                            string lexical =
                                scalar.ToString(
                                    CultureInfo
                                        .InvariantCulture);
                            ValidateNumber(
                                lexical,
                                row.RowId,
                                binding.PropertyName);
                            values[index] =
                                PreparedValue.Number(
                                    lexical);
                            CanonicalValue canonical =
                                CanonicalValue.Int64(
                                    scalar);
                            sourceLogicalValues[index] =
                                canonical;
                            objectByteLength =
                                checked(
                                    objectByteLength +
                                    lexical.Length);
                            break;
                        }

                    case DbType.Real:
                        {
                            double scalar =
                                value.AsReal;
                            if (!double.IsFinite(
                                    scalar))
                            {
                                throw new InvalidDataException(
                                    $"JSON export row {row.RowId}, column '{binding.PropertyName}' contains a non-finite REAL.");
                            }

                            string lexical =
                                scalar.ToString(
                                    "R",
                                    CultureInfo
                                        .InvariantCulture);
                            ValidateNumber(
                                lexical,
                                row.RowId,
                                binding.PropertyName);
                            values[index] =
                                PreparedValue.Number(
                                    lexical);
                            CanonicalValue canonical =
                                CanonicalValue.Binary64(
                                    scalar);
                            sourceLogicalValues[index] =
                                canonical;
                            objectByteLength =
                                checked(
                                    objectByteLength +
                                    lexical.Length);
                            break;
                        }

                    case DbType.Text:
                        {
                            string text = value.AsText;
                            JsonStringMetrics metrics =
                                MeasureJsonString(
                                    text,
                                    JsonInputContracts
                                        .MaximumStringBytes,
                                    $"JSON export row {row.RowId}, column '{binding.PropertyName}'",
                                    cancellationToken);
                            values[index] =
                                PreparedValue.Text(
                                    text);
                            CanonicalValue canonical =
                                CanonicalValue.Text(
                                    text);
                            sourceLogicalValues[index] =
                                canonical;
                            objectByteLength =
                                checked(
                                    objectByteLength +
                                    metrics
                                        .JsonLiteralByteLength);
                            break;
                        }

                    case DbType.Blob:
                        {
                            byte[] sourceBlob =
                                value.AsBlob;
                            if (sourceBlob.Length >
                                binding
                                    .MaximumDecodedBytes)
                            {
                                throw new InvalidDataException(
                                    $"JSON export row {row.RowId}, column '{binding.PropertyName}' BLOB exceeds its {binding.MaximumDecodedBytes}-byte decoded ceiling.");
                            }

                            int encodedLength =
                                checked(
                                    (int)(
                                        ((long)sourceBlob
                                             .Length +
                                         2L) /
                                        3L *
                                        4L));
                            if (encodedLength >
                                JsonInputContracts
                                    .MaximumStringBytes)
                            {
                                throw new InvalidDataException(
                                    $"JSON export row {row.RowId}, column '{binding.PropertyName}' base64 text exceeds the strict reader's string-byte ceiling.");
                            }

                            byte[] blob =
                                sourceBlob.ToArray();
                            values[index] =
                                PreparedValue.Blob(
                                    blob);
                            CanonicalValue canonical =
                                CanonicalValue.Blob(
                                    blob);
                            sourceLogicalValues[index] =
                                canonical;
                            objectByteLength =
                                checked(
                                    objectByteLength +
                                    encodedLength +
                                    2L);
                            break;
                        }

                    default:
                        throw new InvalidOperationException(
                            $"Prepared JSON export type {binding.SourceType} is unsupported.");
                }
            }

            if (objectByteLength >
                JsonInputContracts.MaximumValueBytes)
            {
                throw new InvalidDataException(
                    $"JSON export row {row.RowId} exceeds the strict reader's {JsonInputContracts.MaximumValueBytes}-byte logical-value ceiling.");
            }

            long prefixByteLength =
                request.Framing switch
                {
                    JsonExportFraming.RootArray =>
                        followsRow ? 1L : 0L,
                    JsonExportFraming.Ndjson => 0L,
                    _ =>
                        throw new InvalidOperationException(
                            "Prepared JSON export framing is unsupported."),
                };
            long suffixByteLength =
                request.Framing switch
                {
                    JsonExportFraming.RootArray => 2L,
                    JsonExportFraming.Ndjson => 1L,
                    _ =>
                        throw new InvalidOperationException(
                            "Prepared JSON export framing is unsupported."),
                };
            long requiredToComplete =
                checked(
                    prefixByteLength +
                    objectByteLength +
                    suffixByteLength);
            if (requiredToComplete >
                maximumDataBytes -
                bytesAlreadyWritten)
            {
                throw new InvalidDataException(
                    $"JSON export row {row.RowId} exceeds the configured data-byte ceiling.");
            }

            cancellationToken.ThrowIfCancellationRequested();
            sourceCanonicalRowHash =
                CanonicalRowCodec
                    .ComputeRowHashBytes(
                        sourceLogicalValues);
            cancellationToken.ThrowIfCancellationRequested();

            // Render exactly once into the bounded row buffer. The strict
            // parse-back below derives exported semantics from these bytes,
            // and WriteVerifiedRowAsync later emits these same bytes.
            renderedObject =
                GC.AllocateUninitializedArray<byte>(
                    checked((int)objectByteLength));
            var renderedSink =
                new FixedRowByteSink(
                    renderedObject);
            await RenderPreparedObjectAsync(
                    renderedSink,
                    request,
                    values,
                    cancellationToken)
                .ConfigureAwait(false);
            if (renderedSink.BytesWritten !=
                objectByteLength)
            {
                throw new InvalidOperationException(
                    "The JSON export row renderer did not produce its prevalidated byte length.");
            }

            exportedCanonicalRowHash =
                await ComputeExportedRowHashAsync(
                        renderedObject,
                        request,
                        values,
                        sourceCanonicalRowHash,
                        cancellationToken)
                    .ConfigureAwait(false);
            var result =
                new PreparedRow(
                    renderedObject,
                    sourceCanonicalRowHash,
                    exportedCanonicalRowHash,
                    objectByteLength);
            renderedObject = null;
            sourceCanonicalRowHash = null;
            exportedCanonicalRowHash = null;
            return result;
        }
        catch
        {
            Zero(renderedObject);
            Zero(sourceCanonicalRowHash);
            Zero(exportedCanonicalRowHash);
            throw;
        }
        finally
        {
            ClearPreparedBlobs(values);
            Array.Clear(sourceValues);
        }
    }

    private static async ValueTask
        RenderPreparedObjectAsync(
        IJsonExportByteSink sink,
        PreparedRequest request,
        IReadOnlyList<PreparedValue> values,
        CancellationToken cancellationToken)
    {
        sink.EnsureCanWrite(
            request.MinimumObjectByteLength);
        await sink
            .WriteAsync(
                s_objectStart,
                cancellationToken)
            .ConfigureAwait(false);
        for (int index = 0;
             index < request.Bindings.Length;
             index++)
        {
            if (index != 0)
            {
                await sink
                    .WriteAsync(
                        s_comma,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            await WriteJsonStringAsync(
                    sink,
                    request.Bindings[index]
                        .PropertyName,
                    cancellationToken)
                .ConfigureAwait(false);
            await sink
                .WriteAsync(
                    s_colon,
                    cancellationToken)
                .ConfigureAwait(false);
            await WriteValueAsync(
                    sink,
                    values[index],
                    cancellationToken)
                .ConfigureAwait(false);
        }
        await sink
            .WriteAsync(
                s_objectEnd,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async ValueTask<byte[]>
        ComputeExportedRowHashAsync(
        byte[] renderedObject,
        PreparedRequest request,
        IReadOnlyList<PreparedValue> expectedValues,
        byte[] sourceCanonicalRowHash,
        CancellationToken cancellationToken)
    {
        using var source =
            new MemoryStream(
                renderedObject,
                writable: false);
        await using JsonStreamingReader reader =
            await JsonStreamingReader.OpenAsync(
                    source,
                    new JsonStreamingReaderOptions
                    {
                        Framing =
                            JsonInputFraming
                                .MultipleValues,
                        MaxValueBytes =
                            renderedObject.Length,
                        MaxDepth = 64,
                        MaxPropertiesPerObject =
                            request.Bindings.Length,
                        MaxArrayElements = 1,
                        MaxTotalNodes =
                            checked(
                                request.Bindings.Length +
                                1),
                        MaxPropertyNameBytes =
                            JsonInputContracts
                                .MaximumPropertyNameBytes,
                        MaxStringBytes =
                            JsonInputContracts
                                .MaximumStringBytes,
                        MaxNumberBytes =
                            JsonInputContracts
                                .MaximumNumberBytes,
                        LeaveOpen = true,
                    },
                    cancellationToken)
                .ConfigureAwait(false);

        JsonLogicalRecord? renderedRecord =
            null;
        await foreach (
            JsonLogicalRecord candidate in
            reader.ReadValuesAsync(
                    cancellationToken)
                .ConfigureAwait(false))
        {
            if (renderedRecord is not null)
            {
                throw new InvalidDataException(
                    "The JSON export row renderer produced more than one logical value.");
            }

            renderedRecord = candidate;
        }

        if (renderedRecord is null ||
            renderedRecord.RecordOrdinal != 1 ||
            renderedRecord.StartByteOffset != 0 ||
            renderedRecord.RawByteLength !=
                renderedObject.LongLength)
        {
            throw new InvalidDataException(
                "The JSON export row renderer did not produce one exact compact logical value.");
        }

        JsonLogicalValue renderedValue =
            renderedRecord.Value;
        if (renderedValue.Kind !=
            JsonLogicalValueKind.Object)
        {
            throw new InvalidDataException(
                "The JSON export row renderer did not produce an object.");
        }

        IReadOnlyList<JsonLogicalProperty> properties =
            renderedValue.Properties;
        if (properties.Count !=
            request.Bindings.Length)
        {
            throw new InvalidDataException(
                "The rendered JSON export object does not match the source schema width.");
        }

        var exportedLogicalValues =
            new CanonicalValue[
                request.Bindings.Length];
        var decodedBlobs =
            new List<byte[]>(
                request.Bindings.Length);
        byte[]? exportedCanonicalRowHash = null;
        try
        {
            for (int index = 0;
                 index < request.Bindings.Length;
                 index++)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                ColumnBinding binding =
                    request.Bindings[index];
                JsonLogicalProperty property =
                    properties[index];
                if (property.Ordinal != index ||
                    !string.Equals(
                        property.Name,
                        binding.PropertyName,
                        StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        "The rendered JSON export object does not preserve schema property order and names.");
                }

                PreparedValue expected =
                    expectedValues[index];
                JsonLogicalValue actual =
                    property.Value;
                switch (expected.Kind)
                {
                    case PreparedValueKind.Null:
                        if (actual.Kind !=
                            JsonLogicalValueKind.Null)
                        {
                            throw RenderedValueMismatch(
                                binding.PropertyName);
                        }
                        exportedLogicalValues[index] =
                            CanonicalValue.Null(
                                binding.CanonicalType);
                        break;

                    case PreparedValueKind.Number:
                        if (actual.Kind !=
                                JsonLogicalValueKind
                                    .Number ||
                            !string.Equals(
                                actual.NumberLexeme,
                                expected.TextValue,
                                StringComparison.Ordinal))
                        {
                            throw RenderedValueMismatch(
                                binding.PropertyName);
                        }

                        exportedLogicalValues[index] =
                            binding.SourceType switch
                            {
                                DbType.Integer =>
                                    ParseRenderedInteger(
                                        actual.NumberLexeme,
                                        binding.PropertyName),
                                DbType.Real =>
                                    ParseRenderedReal(
                                        actual.NumberLexeme,
                                        binding.PropertyName),
                                _ => throw new InvalidOperationException(
                                    "A rendered JSON number has a nonnumeric source type."),
                            };
                        break;

                    case PreparedValueKind.Text:
                        if (actual.Kind !=
                                JsonLogicalValueKind
                                    .String ||
                            !string.Equals(
                                actual.StringValue,
                                expected.TextValue,
                                StringComparison.Ordinal))
                        {
                            throw RenderedValueMismatch(
                                binding.PropertyName);
                        }
                        exportedLogicalValues[index] =
                            CanonicalValue.Text(
                                actual.StringValue);
                        break;

                    case PreparedValueKind.Blob:
                        if (actual.Kind !=
                            JsonLogicalValueKind.String)
                        {
                            throw RenderedValueMismatch(
                                binding.PropertyName);
                        }

                        byte[] decoded =
                            DecodeRenderedBlob(
                                actual.StringValue,
                                binding.MaximumDecodedBytes,
                                binding.PropertyName);
                        try
                        {
                            decodedBlobs.Add(decoded);
                        }
                        catch
                        {
                            Zero(decoded);
                            throw;
                        }
                        if (!decoded.AsSpan()
                            .SequenceEqual(
                                expected.BlobValue))
                        {
                            throw RenderedValueMismatch(
                                binding.PropertyName);
                        }
                        exportedLogicalValues[index] =
                            CanonicalValue.Blob(
                                decoded);
                        break;

                    default:
                        throw new InvalidOperationException(
                            "A prepared JSON export value kind is unsupported.");
                }
            }

            exportedCanonicalRowHash =
                CanonicalRowCodec
                    .ComputeRowHashBytes(
                        exportedLogicalValues);
            if (!CryptographicOperations
                .FixedTimeEquals(
                    sourceCanonicalRowHash,
                    exportedCanonicalRowHash))
            {
                throw new InvalidDataException(
                    "The rendered JSON export row does not preserve the source logical values.");
            }

            byte[] result =
                exportedCanonicalRowHash;
            exportedCanonicalRowHash = null;
            return result;
        }
        finally
        {
            Zero(exportedCanonicalRowHash);
            foreach (byte[] decoded in
                     decodedBlobs)
            {
                Zero(decoded);
            }
        }
    }

    private static CanonicalValue ParseRenderedInteger(
        string lexical,
        string propertyName)
    {
        if (!long.TryParse(
                lexical,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out long value))
        {
            throw new InvalidDataException(
                $"Rendered JSON export column '{propertyName}' is not a signed 64-bit integer.");
        }

        return CanonicalValue.Int64(value);
    }

    private static CanonicalValue ParseRenderedReal(
        string lexical,
        string propertyName)
    {
        if (!double.TryParse(
                lexical,
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out double value) ||
            !double.IsFinite(value))
        {
            throw new InvalidDataException(
                $"Rendered JSON export column '{propertyName}' is not a finite binary64 value.");
        }

        return CanonicalValue.Binary64(value);
    }

    private static byte[] DecodeRenderedBlob(
        string encoded,
        int maximumDecodedBytes,
        string propertyName)
    {
        if ((encoded.Length & 3) != 0)
        {
            throw new InvalidDataException(
                $"Rendered JSON export column '{propertyName}' is not padded RFC 4648 base64.");
        }

        int padding =
            encoded.EndsWith(
                "==",
                StringComparison.Ordinal)
                ? 2
                : encoded.EndsWith(
                    "=",
                    StringComparison.Ordinal)
                    ? 1
                    : 0;
        int decodedLength =
            checked(
                encoded.Length / 4 * 3 -
                padding);
        if (decodedLength >
            maximumDecodedBytes)
        {
            throw new InvalidDataException(
                $"Rendered JSON export column '{propertyName}' exceeds its decoded BLOB ceiling.");
        }

        byte[] decoded =
            GC.AllocateUninitializedArray<byte>(
                decodedLength);
        try
        {
            if (!Convert.TryFromBase64Chars(
                    encoded,
                    decoded,
                    out int written) ||
                written != decodedLength ||
                !string.Equals(
                    Convert.ToBase64String(
                        decoded),
                    encoded,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Rendered JSON export column '{propertyName}' is not canonical padded RFC 4648 base64.");
            }

            return decoded;
        }
        catch
        {
            Zero(decoded);
            throw;
        }
    }

    private static InvalidDataException
        RenderedValueMismatch(
        string propertyName) =>
        new(
            $"Rendered JSON export column '{propertyName}' does not preserve its prepared logical value.");

    private static async ValueTask WriteVerifiedRowAsync(
        ExportByteSink sink,
        PreparedRequest request,
        PreparedRow row,
        bool followsRow,
        CancellationToken cancellationToken)
    {
        long bytesToWrite =
            checked(
                row.ObjectByteLength +
                (request.Framing ==
                     JsonExportFraming.Ndjson
                    ? 1L
                    : followsRow
                        ? 1L
                        : 0L));
        sink.EnsureCanWrite(bytesToWrite);

        if (request.Framing ==
                JsonExportFraming.RootArray &&
            followsRow)
        {
            await sink
                .WriteAsync(
                    s_comma,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        int offset = 0;
        while (offset <
               row.RenderedObjectBytes.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = Math.Min(
                Utf8BufferBytes,
                row.RenderedObjectBytes.Length -
                offset);
            await sink.WriteAsync(
                    row.RenderedObjectBytes.AsMemory(
                        offset,
                        count),
                    cancellationToken)
                .ConfigureAwait(false);
            offset += count;
        }

        if (request.Framing ==
            JsonExportFraming.Ndjson)
        {
            await sink
                .WriteAsync(
                    s_newline,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async ValueTask WriteValueAsync(
        IJsonExportByteSink sink,
        PreparedValue value,
        CancellationToken cancellationToken)
    {
        switch (value.Kind)
        {
            case PreparedValueKind.Null:
                await sink
                    .WriteAsync(
                        s_null,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PreparedValueKind.Number:
                await WriteUtf8Async(
                        sink,
                        value.TextValue!,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PreparedValueKind.Text:
                await WriteJsonStringAsync(
                        sink,
                        value.TextValue!,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PreparedValueKind.Blob:
                await sink
                    .WriteAsync(
                        s_quote,
                        cancellationToken)
                    .ConfigureAwait(false);
                await WriteBlobAsync(
                        sink,
                        value.BlobValue!,
                        cancellationToken)
                    .ConfigureAwait(false);
                await sink
                    .WriteAsync(
                        s_quote,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            default:
                throw new InvalidOperationException(
                    "Prepared JSON export value kind is unsupported.");
        }
    }

    private static async ValueTask WriteJsonStringAsync(
        IJsonExportByteSink sink,
        string value,
        CancellationToken cancellationToken)
    {
        await sink
            .WriteAsync(
                s_quote,
                cancellationToken)
            .ConfigureAwait(false);

        int offset = 0;
        for (int index = 0;
             index < value.Length;
             index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            char character = value[index];
            ReadOnlyMemory<byte> escape;
            if (character == '"')
            {
                escape = s_escapedQuote;
            }
            else if (character == '\\')
            {
                escape = s_escapedBackslash;
            }
            else if (character < 0x20)
            {
                escape =
                    s_controlEscapes[character];
            }
            else
            {
                continue;
            }

            if (index != offset)
            {
                await WriteUtf8Async(
                        sink,
                        value.AsMemory(
                            offset,
                            index - offset),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            await sink
                .WriteAsync(
                    escape,
                    cancellationToken)
                .ConfigureAwait(false);
            offset = index + 1;
        }

        if (offset != value.Length)
        {
            await WriteUtf8Async(
                    sink,
                    value.AsMemory(offset),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        await sink
            .WriteAsync(
                s_quote,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async ValueTask WriteUtf8Async(
        IJsonExportByteSink sink,
        string value,
        CancellationToken cancellationToken) =>
        await WriteUtf8Async(
                sink,
                value.AsMemory(),
                cancellationToken)
            .ConfigureAwait(false);

    private static async ValueTask WriteUtf8Async(
        IJsonExportByteSink sink,
        ReadOnlyMemory<char> value,
        CancellationToken cancellationToken)
    {
        if (value.IsEmpty)
            return;

        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                Utf8BufferBytes);
        try
        {
            int consumedCharacters = 0;
            while (consumedCharacters <
                   value.Length)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                int charactersUsed = Math.Min(
                    Utf8InputChunkCharacters,
                    value.Length -
                    consumedCharacters);
                int boundary =
                    consumedCharacters +
                    charactersUsed;
                if (boundary < value.Length &&
                    char.IsHighSurrogate(
                        value.Span[
                            boundary - 1]))
                {
                    charactersUsed--;
                }
                if (charactersUsed <= 0)
                {
                    throw new InvalidOperationException(
                        "Strict UTF-8 chunking made no progress.");
                }

                int bytesUsed =
                    s_strictUtf8.GetBytes(
                        value.Span.Slice(
                            consumedCharacters,
                            charactersUsed),
                        buffer);
                consumedCharacters +=
                    charactersUsed;
                await sink
                    .WriteAsync(
                        buffer.AsMemory(
                            0,
                            bytesUsed),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(
                buffer,
                clearArray: true);
        }
    }

    private static async ValueTask WriteBlobAsync(
        IJsonExportByteSink sink,
        byte[] blob,
        CancellationToken cancellationToken)
    {
        if (blob.Length == 0)
            return;

        byte[] output =
            ArrayPool<byte>.Shared.Rent(
                Base64
                    .GetMaxEncodedToUtf8Length(
                        BlobInputChunkBytes));
        try
        {
            int offset = 0;
            while (offset < blob.Length)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                int count = Math.Min(
                    BlobInputChunkBytes,
                    blob.Length - offset);
                bool final =
                    offset + count ==
                    blob.Length;
                OperationStatus status =
                    Base64.EncodeToUtf8(
                        blob.AsSpan(offset, count),
                        output,
                        out int consumed,
                        out int written,
                        final);
                if (status !=
                        OperationStatus.Done ||
                    consumed != count)
                {
                    throw new InvalidOperationException(
                        "The deterministic padded base64 encoder did not complete.");
                }

                await sink
                    .WriteAsync(
                        output.AsMemory(
                            0,
                            written),
                        cancellationToken)
                    .ConfigureAwait(false);
                offset += consumed;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(
                output,
                clearArray: true);
        }
    }

    private static JsonStringMetrics MeasureJsonString(
        string value,
        int maximumDecodedUtf8Bytes,
        string description,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(value);
        long decodedBytes = 0;
        long escapedBytes = 0;

        for (int index = 0;
             index < value.Length;
             index++)
        {
            if ((index & 0xfff) == 0)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
            }

            char character = value[index];
            if (char.IsHighSurrogate(character))
            {
                if (index + 1 >= value.Length ||
                    !char.IsLowSurrogate(
                        value[index + 1]))
                {
                    throw new InvalidDataException(
                        $"{description} contains invalid Unicode.");
                }

                decodedBytes =
                    checked(decodedBytes + 4L);
                escapedBytes =
                    checked(escapedBytes + 4L);
                index++;
            }
            else if (char.IsLowSurrogate(
                         character))
            {
                throw new InvalidDataException(
                    $"{description} contains invalid Unicode.");
            }
            else
            {
                int utf8Bytes =
                    character switch
                    {
                        <= '\u007f' => 1,
                        <= '\u07ff' => 2,
                        _ => 3,
                    };
                decodedBytes =
                    checked(
                        decodedBytes +
                        utf8Bytes);
                escapedBytes =
                    checked(
                        escapedBytes +
                        EscapedByteLength(
                            character,
                            utf8Bytes));
            }

            if (decodedBytes >
                maximumDecodedUtf8Bytes)
            {
                throw new InvalidDataException(
                    $"{description} exceeds the strict reader's {maximumDecodedUtf8Bytes}-byte decoded UTF-8 ceiling.");
            }
        }

        return new JsonStringMetrics(
            checked((int)decodedBytes),
            checked(escapedBytes + 2L));
    }

    private static int EscapedByteLength(
        char character,
        int utf8Bytes)
    {
        if (character is '"' or '\\')
            return 2;
        if (character >= 0x20)
            return utf8Bytes;
        return character is
            '\b' or '\t' or '\n' or '\f' or '\r'
                ? 2
                : 6;
    }

    private static void ValidateNumber(
        string lexical,
        long rowId,
        string propertyName)
    {
        if (lexical.Length >
                JsonInputContracts
                    .MaximumNumberBytes ||
            !JsonNumberLexeme.IsValid(lexical))
        {
            throw new InvalidDataException(
                $"JSON export row {rowId}, column '{propertyName}' did not produce a valid bounded JSON number.");
        }
    }

    private static void ValidateManifestText(
        string value,
        string description)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidDataException(
                $"{description} must be nonblank.");
        }
        if (value.Contains(
                '\0',
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"{description} contains a NUL character.");
        }

        _ = MeasureJsonString(
            value,
            JsonInputContracts
                .MaximumStringBytes,
            description,
            CancellationToken.None);
    }

    private static (
        JsonExportDatabaseType DatabaseType,
        CanonicalType CanonicalType,
        string ValueEncoding)
        MapType(DbType type) =>
        type switch
        {
            DbType.Integer => (
                JsonExportDatabaseType.Integer,
                CanonicalType.Int64,
                JsonExportContracts
                    .IntegerValueEncoding),
            DbType.Real => (
                JsonExportDatabaseType.Real,
                CanonicalType.Binary64,
                JsonExportContracts
                    .RealValueEncoding),
            DbType.Text => (
                JsonExportDatabaseType.Text,
                CanonicalType.Text,
                JsonExportContracts
                    .TextValueEncoding),
            DbType.Blob => (
                JsonExportDatabaseType.Blob,
                CanonicalType.Blob,
                JsonExportContracts
                    .BlobValueEncoding),
            _ => throw new InvalidDataException(
                $"JSON export source type {type} is unsupported."),
        };

    private static JsonExportSourceManifest CopySource(
        JsonExportSourceManifest source)
    {
        JsonExportHashManifest digest =
            source.SnapshotDigest is null
                ? null!
                : new JsonExportHashManifest
                {
                    Algorithm =
                        source.SnapshotDigest
                            .Algorithm,
                    Value =
                        source.SnapshotDigest.Value,
                };
        return new JsonExportSourceManifest
        {
            Kind = source.Kind,
            Version = source.Version,
            SnapshotByteLength =
                source.SnapshotByteLength,
            SnapshotDigest = digest,
        };
    }

    private static JsonExportFormatManifest CreateFormat(
        JsonStreamingExportRequest request) =>
        new()
        {
            Encoding = JsonExportContracts.Encoding,
            HasByteOrderMark = false,
            Culture = JsonExportContracts.Culture,
            Framing = request.Framing,
            Compact = true,
            PropertyOrder =
                JsonExportContracts.PropertyOrder,
            Newline = JsonExportContracts.Newline,
            HasFinalNewline = true,
            NullEncoding =
                JsonExportContracts.NullEncoding,
            TextEscape =
                JsonExportContracts.TextEscape,
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes =
                request.MaximumDecodedBlobBytes,
            MaximumValueBytes =
                JsonInputContracts.MaximumValueBytes,
            MaximumStringBytes =
                JsonInputContracts.MaximumStringBytes,
            MaximumPropertyNameBytes =
                JsonInputContracts
                    .MaximumPropertyNameBytes,
            MaximumPropertiesPerObject =
                JsonInputContracts
                    .MaximumPropertiesPerObject,
        };

    private static JsonExportManifest
        CreateProvisionalManifest(
            PreparedRequest request)
    {
        ReadOnlySpan<byte> emptyData =
            request.Framing ==
                JsonExportFraming.RootArray
                ? "[]\n"u8
                : ReadOnlySpan<byte>.Empty;
        byte[] physicalDigestBytes =
            SHA256.HashData(emptyData);
        JsonExportHashManifest physicalDigest;
        try
        {
            physicalDigest =
                new JsonExportHashManifest
                {
                    Algorithm =
                        JsonExportHashManifest
                            .Sha256Algorithm,
                    Value =
                        Convert.ToHexString(
                                physicalDigestBytes)
                            .ToLowerInvariant(),
                };
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                physicalDigestBytes);
        }

        using var logicalDigestBuilder =
            new JsonExportOrderedContentDigest();
        JsonExportHashManifest logicalDigest =
            logicalDigestBuilder.Complete();
        return new JsonExportManifest
        {
            Profile = request.Profile,
            Source = request.Source,
            Table = request.Table,
            Json = request.Format,
            Content =
                new JsonExportContentManifest
                {
                    RowCount = 0,
                    DataByteLength =
                        request.Framing ==
                        JsonExportFraming.RootArray
                            ? 3
                            : 0,
                    DataDigest =
                        physicalDigest,
                    Canonicalization =
                        JsonExportContracts
                            .Canonicalization,
                    CanonicalizationContractDigest =
                        JsonExportContracts
                            .CanonicalizationContractDigest,
                    Aggregation =
                        JsonExportContracts
                            .OrderedContentDigest,
                    SourceLogicalDigest =
                        logicalDigest,
                    ExportedLogicalDigest =
                        logicalDigest with { },
                },
        };
    }

    private static JsonExportManifest CreateManifest(
        PreparedRequest request,
        long rowCount,
        long dataByteLength,
        JsonExportHashManifest dataDigest,
        JsonExportHashManifest sourceLogicalDigest,
        JsonExportHashManifest exportedLogicalDigest) =>
        new()
        {
            Profile = request.Profile,
            Source = request.Source,
            Table = request.Table,
            Json = request.Format,
            Content =
                new JsonExportContentManifest
                {
                    RowCount = rowCount,
                    DataByteLength =
                        dataByteLength,
                    DataDigest = dataDigest,
                    Canonicalization =
                        JsonExportContracts
                            .Canonicalization,
                    CanonicalizationContractDigest =
                        JsonExportContracts
                            .CanonicalizationContractDigest,
                    Aggregation =
                        JsonExportContracts
                            .OrderedContentDigest,
                    SourceLogicalDigest =
                        sourceLogicalDigest,
                    ExportedLogicalDigest =
                        exportedLogicalDigest,
                },
        };

    private static bool HashEquals(
        JsonExportHashManifest left,
        JsonExportHashManifest right) =>
        string.Equals(
            left.Algorithm,
            right.Algorithm,
            StringComparison.Ordinal) &&
        string.Equals(
            left.Value,
            right.Value,
            StringComparison.Ordinal);

    private static void ValidateRestartOnlyDestination(
        Stream destination)
    {
        if (!destination.CanWrite)
        {
            throw new ArgumentException(
                "JSON export destination must be writable.",
                nameof(destination));
        }
        if (!destination.CanSeek)
        {
            throw new ArgumentException(
                "JSON export destination must be seekable.",
                nameof(destination));
        }

        long position;
        long length;
        try
        {
            position = destination.Position;
            length = destination.Length;
        }
        catch (Exception exception) when (
            exception is
                NotSupportedException or
                IOException or
                ObjectDisposedException)
        {
            throw new ArgumentException(
                "JSON export destination must expose its current position and length.",
                nameof(destination),
                exception);
        }

        if (position != 0 || length != 0)
        {
            throw new ArgumentException(
                "JSON export destination must be empty and positioned at byte zero.",
                nameof(destination));
        }
    }

    private static ReadOnlyMemory<byte>[]
        CreateControlEscapes()
    {
        var result =
            new ReadOnlyMemory<byte>[0x20];
        const string hex = "0123456789abcdef";
        for (int value = 0;
             value < result.Length;
             value++)
        {
            string escape = value switch
            {
                '\b' => "\\b",
                '\t' => "\\t",
                '\n' => "\\n",
                '\f' => "\\f",
                '\r' => "\\r",
                _ =>
                    "\\u00" +
                    hex[value >> 4] +
                    hex[value & 0x0f],
            };
            result[value] =
                Encoding.ASCII.GetBytes(
                    escape);
        }
        return result;
    }

    private static void ClearPreparedBlobs(
        IEnumerable<PreparedValue> values)
    {
        foreach (PreparedValue value in values)
        {
            if (value.Kind ==
                    PreparedValueKind.Blob &&
                value.BlobValue is not null)
            {
                CryptographicOperations.ZeroMemory(
                    value.BlobValue);
            }
        }
    }

    private static void Zero(byte[]? value)
    {
        if (value is not null)
        {
            CryptographicOperations.ZeroMemory(
                value);
        }
    }

    private sealed record PreparedRequest(
        JsonExportProfile Profile,
        JsonExportFraming Framing,
        JsonExportSourceManifest Source,
        JsonExportTableManifest Table,
        JsonExportFormatManifest Format,
        ColumnBinding[] Bindings,
        long MinimumObjectByteLength);

    private sealed record ColumnBinding(
        int Ordinal,
        string PropertyName,
        DbType SourceType,
        CanonicalType CanonicalType,
        bool Nullable,
        int MaximumDecodedBytes,
        long PropertySyntaxByteLength);

    private sealed record PreparedRow(
        byte[] RenderedObjectBytes,
        byte[] SourceCanonicalRowHash,
        byte[] ExportedCanonicalRowHash,
        long ObjectByteLength)
    {
        internal void ClearSensitiveBuffers()
        {
            CryptographicOperations.ZeroMemory(
                RenderedObjectBytes);
            CryptographicOperations.ZeroMemory(
                SourceCanonicalRowHash);
            CryptographicOperations.ZeroMemory(
                ExportedCanonicalRowHash);
        }
    }

    private enum PreparedValueKind
    {
        Null,
        Number,
        Text,
        Blob,
    }

    private readonly record struct PreparedValue(
        PreparedValueKind Kind,
        string? TextValue,
        byte[]? BlobValue)
    {
        internal static PreparedValue Null { get; } =
            new(
                PreparedValueKind.Null,
                null,
                null);

        internal static PreparedValue Number(
            string value) =>
            new(
                PreparedValueKind.Number,
                value,
                null);

        internal static PreparedValue Text(
            string value) =>
            new(
                PreparedValueKind.Text,
                value,
                null);

        internal static PreparedValue Blob(
            byte[] value) =>
            new(
                PreparedValueKind.Blob,
                null,
                value);
    }

    private readonly record struct JsonStringMetrics(
        int DecodedUtf8ByteLength,
        long JsonLiteralByteLength);

    private interface IJsonExportByteSink
    {
        long BytesWritten { get; }

        void EnsureCanWrite(long byteCount);

        ValueTask WriteAsync(
            ReadOnlyMemory<byte> bytes,
            CancellationToken cancellationToken);
    }

    private sealed class FixedRowByteSink :
        IJsonExportByteSink
    {
        private readonly byte[] buffer;

        internal FixedRowByteSink(
            byte[] buffer)
        {
            ArgumentNullException.ThrowIfNull(
                buffer);
            this.buffer = buffer;
        }

        public long BytesWritten { get; private set; }

        public void EnsureCanWrite(
            long byteCount)
        {
            if (byteCount < 0 ||
                byteCount >
                buffer.LongLength -
                BytesWritten)
            {
                throw new InvalidOperationException(
                    "The JSON export row renderer exceeded its prevalidated byte length.");
            }
        }

        public ValueTask WriteAsync(
            ReadOnlyMemory<byte> bytes,
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            EnsureCanWrite(bytes.Length);
            bytes.Span.CopyTo(
                buffer.AsSpan(
                    checked((int)BytesWritten)));
            BytesWritten =
                checked(
                    BytesWritten +
                    bytes.Length);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ExportByteSink :
        IJsonExportByteSink,
        IDisposable
    {
        private readonly Stream destination;
        private readonly long maximumBytes;
        private readonly IncrementalHash hash;
        private bool completed;
        private bool disposed;

        internal ExportByteSink(
            Stream destination,
            long maximumBytes)
        {
            ArgumentNullException.ThrowIfNull(
                destination);
            if (maximumBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maximumBytes));
            }

            this.destination = destination;
            this.maximumBytes = maximumBytes;
            hash =
                IncrementalHash.CreateHash(
                    HashAlgorithmName.SHA256);
        }

        internal ExportByteSink(
            Stream destination,
            long maximumBytes,
            IncrementalHash hash,
            long bytesWritten)
        {
            ArgumentNullException.ThrowIfNull(
                destination);
            ArgumentNullException.ThrowIfNull(
                hash);
            if (maximumBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maximumBytes));
            }
            if (bytesWritten < 0 ||
                bytesWritten > maximumBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(bytesWritten));
            }
            if (hash.AlgorithmName !=
                HashAlgorithmName.SHA256)
            {
                throw new ArgumentException(
                    "The seeded JSON export checksum must use SHA-256.",
                    nameof(hash));
            }

            this.destination = destination;
            this.maximumBytes = maximumBytes;
            this.hash = hash;
            BytesWritten = bytesWritten;
        }

        public long BytesWritten
        {
            get;
            private set;
        }

        public void EnsureCanWrite(
            long byteCount)
        {
            ThrowIfUnavailable();
            if (byteCount < 0 ||
                byteCount >
                maximumBytes - BytesWritten)
            {
                throw new InvalidDataException(
                    "JSON export exceeds its configured data-byte ceiling.");
            }
        }

        public async ValueTask WriteAsync(
            ReadOnlyMemory<byte> bytes,
            CancellationToken cancellationToken)
        {
            EnsureCanWrite(bytes.Length);
            await destination
                .WriteAsync(
                    bytes,
                    cancellationToken)
                .ConfigureAwait(false);
            hash.AppendData(bytes.Span);
            BytesWritten =
                checked(
                    BytesWritten +
                    bytes.Length);
        }

        internal JsonExportHashManifest
            GetCurrentHash()
        {
            ThrowIfUnavailable();
            byte[] current =
                hash.GetCurrentHash();
            try
            {
                return new JsonExportHashManifest
                {
                    Algorithm =
                        JsonExportHashManifest
                            .Sha256Algorithm,
                    Value =
                        Convert.ToHexString(
                                current)
                            .ToLowerInvariant(),
                };
            }
            finally
            {
                CryptographicOperations.ZeroMemory(
                    current);
            }
        }

        internal JsonExportHashManifest
            CompleteHash()
        {
            JsonExportHashManifest result =
                GetCurrentHash();
            completed = true;
            return result;
        }

        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;
            hash.Dispose();
            GC.SuppressFinalize(this);
        }

        private void ThrowIfUnavailable()
        {
            ObjectDisposedException.ThrowIf(
                disposed,
                this);
            if (completed)
            {
                throw new InvalidOperationException(
                    "JSON export data checksum is already complete.");
            }
        }
    }
}
