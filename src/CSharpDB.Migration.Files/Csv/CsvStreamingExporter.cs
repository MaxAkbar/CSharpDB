using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// One physical source row supplied in strictly increasing row-ID order.
/// The row ID establishes order only and is not written or hashed.
/// </summary>
public readonly record struct CsvExportRow(
    long RowId,
    ReadOnlyMemory<DbValue> Values);

/// <summary>
/// Complete, restart-only input for one deterministic streaming CSV export.
/// </summary>
public sealed record CsvStreamingExportRequest
{
    public required CsvExportProfile Profile { get; init; }

    public required CsvExportSourceManifest Source { get; init; }

    public required TableSchema Table { get; init; }

    public required IAsyncEnumerable<CsvExportRow> Rows { get; init; }

    /// <summary>Maximum number of CSV bytes that may be emitted.</summary>
    public long MaxDataBytes { get; init; } = 1L << 40;

    /// <summary>
    /// Per-value decoded BLOB ceiling recorded in every BLOB column contract.
    /// </summary>
    public int MaximumDecodedBlobBytes { get; init; } =
        CsvExportContracts.MaximumSupportedDecodedBlobBytes;
}

/// <summary>
/// Successful CSV output evidence. A failed export returns no result; any
/// partial destination bytes remain caller-owned and must be restarted at zero.
/// </summary>
public sealed record CsvStreamingExportResult
{
    public required CsvExportManifest Manifest { get; init; }

    public required byte[] CanonicalManifestBytes { get; init; }

    public required string ManifestDigest { get; init; }
}

/// <summary>
/// Writes deterministic RFC 4180-compatible CSV to a caller-owned empty stream.
/// The exporter flushes but does not claim durable storage.
/// </summary>
public sealed partial class CsvStreamingExporter
{
    private const int Utf8BufferBytes = 16 * 1024;
    private const int Utf8InputChunkCharacters = 4 * 1024;
    private const int BlobInputChunkBytes = 12 * 1024;

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private static readonly ReadOnlyMemory<byte> s_comma = new byte[] { (byte)',' };
    private static readonly ReadOnlyMemory<byte> s_quote = new byte[] { (byte)'"' };
    private static readonly ReadOnlyMemory<byte> s_escapedQuote =
        new byte[] { (byte)'"', (byte)'"' };
    private static readonly ReadOnlyMemory<byte> s_crlf =
        new byte[] { (byte)'\r', (byte)'\n' };
    private static readonly ReadOnlyMemory<byte> s_nullToken =
        Encoding.ASCII.GetBytes(CsvExportContracts.NullToken);

    public async ValueTask<CsvStreamingExportResult> WriteAsync(
        Stream destination,
        CsvStreamingExportRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(request);

        PreparedRequest prepared = PrepareRequest(destination, request);
        cancellationToken.ThrowIfCancellationRequested();

        using var sink = new ExportByteSink(destination, request.MaxDataBytes);
        await WriteHeaderAsync(sink, prepared, cancellationToken).ConfigureAwait(false);

        using var sourceDigest = new CsvExportOrderedContentDigest();
        using var exportedDigest = new CsvExportOrderedContentDigest();
        long transformedRows = 0;
        long transformedCells = 0;
        long rowCount = 0;
        bool hasPreviousRowId = false;
        long previousRowId = 0;

        await foreach (CsvExportRow row in request.Rows
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            if (hasPreviousRowId && row.RowId <= previousRowId)
            {
                throw new InvalidDataException(
                    "CSV export rows must have strictly increasing physical row IDs.");
            }

            PreparedRow preparedRow = PrepareRow(
                row,
                prepared,
                sink.BytesWritten,
                request.MaxDataBytes);
            try
            {
                await WriteRowAsync(sink, preparedRow, cancellationToken).ConfigureAwait(false);

                sourceDigest.AppendRow(preparedRow.SourceLogicalValues);
                exportedDigest.AppendRow(preparedRow.ExportedLogicalValues);
                if (preparedRow.TransformedCellCount != 0)
                {
                    transformedRows = checked(transformedRows + 1);
                    transformedCells = checked(
                        transformedCells + preparedRow.TransformedCellCount);
                }

                rowCount = checked(rowCount + 1);
                previousRowId = row.RowId;
                hasPreviousRowId = true;
            }
            finally
            {
                preparedRow.ClearSensitiveBuffers();
            }
        }

        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);

        CsvExportHashManifest sourceLogicalDigest = sourceDigest.Complete();
        CsvExportHashManifest exportedLogicalDigest = exportedDigest.Complete();
        CsvExportHashManifest dataDigest = sink.CompleteHash();

        if (sourceDigest.RowCount != rowCount || exportedDigest.RowCount != rowCount)
            throw new InvalidOperationException("CSV export logical row counts diverged.");

        CsvExportManifest manifest = CreateManifest(
            prepared,
            rowCount,
            sink.BytesWritten,
            dataDigest,
            sourceLogicalDigest,
            exportedLogicalDigest,
            transformedRows,
            transformedCells);
        byte[] canonicalManifestBytes = CsvExportManifestSerializer.Serialize(manifest);
        string manifestDigest = CsvExportManifestSerializer.ComputeManifestDigest(manifest);

        return new CsvStreamingExportResult
        {
            Manifest = manifest,
            CanonicalManifestBytes = canonicalManifestBytes,
            ManifestDigest = manifestDigest,
        };
    }

    private static PreparedRequest PrepareRequest(
        Stream destination,
        CsvStreamingExportRequest request)
    {
        ValidateRestartOnlyDestination(destination);
        return PrepareRequest(request);
    }

    private static void ValidateRestartOnlyDestination(Stream destination)
    {
        if (!destination.CanWrite)
            throw new ArgumentException("CSV export destination must be writable.", nameof(destination));
        if (!destination.CanSeek)
            throw new ArgumentException("CSV export destination must be seekable.", nameof(destination));

        long position;
        long length;
        try
        {
            position = destination.Position;
            length = destination.Length;
        }
        catch (Exception exception) when (
            exception is NotSupportedException or IOException or ObjectDisposedException)
        {
            throw new ArgumentException(
                "CSV export destination must expose its current position and length.",
                nameof(destination),
                exception);
        }

        if (position != 0 || length != 0)
        {
            throw new ArgumentException(
                "CSV export destination must be empty and positioned at byte zero.",
                nameof(destination));
        }
    }

    private static PreparedRequest PrepareRequest(CsvStreamingExportRequest request)
    {
        if (request.Rows is null)
            throw new ArgumentException("CSV export rows are required.", nameof(request));
        if (request.Table is null)
            throw new ArgumentException("CSV export table schema is required.", nameof(request));
        if (request.Source is null)
            throw new ArgumentException("CSV export source evidence is required.", nameof(request));
        if (!Enum.IsDefined(request.Profile))
            throw new ArgumentOutOfRangeException(nameof(request), "CSV export profile is unsupported.");
        if (request.MaxDataBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "CSV export byte ceiling must be positive.");
        if (request.MaximumDecodedBlobBytes is < 1 or >
            CsvExportContracts.MaximumSupportedDecodedBlobBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "CSV export decoded BLOB ceiling is outside the supported range.");
        }

        string tableName = request.Table.TableName
            ?? throw new ArgumentException("CSV export table name is required.", nameof(request));
        IReadOnlyList<ColumnDefinition> sourceColumns = request.Table.Columns
            ?? throw new ArgumentException("CSV export columns are required.", nameof(request));
        if (sourceColumns.Count is < 1 or > CsvReaderOptions.MaximumSupportedFieldsPerRecord)
        {
            throw new InvalidDataException(
                $"CSV export column count must be between 1 and " +
                $"{CsvReaderOptions.MaximumSupportedFieldsPerRecord}.");
        }

        var bindings = new ColumnBinding[sourceColumns.Count];
        var manifestColumns = new CsvExportColumnManifest[sourceColumns.Count];
        var renderedHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int transformedHeaderCount = 0;
        long headerSyntaxCharacters = sourceColumns.Count - 1L;
        long headerByteLength = sourceColumns.Count - 1L + 2L;

        for (int index = 0; index < sourceColumns.Count; index++)
        {
            ColumnDefinition column = sourceColumns[index]
                ?? throw new InvalidDataException(
                    $"CSV export schema column {index} is missing.");
            string sourceName = column.Name
                ?? throw new InvalidDataException(
                    $"CSV export schema column {index} has no name.");
            ValidateFieldCharacterLimit(
                sourceName.Length,
                $"CSV export source header {index}");
            ValidateManifestText(sourceName, $"CSV export source header {index}");
            string header = request.Profile == CsvExportProfile.LosslessV1
                ? sourceName
                : CsvSpreadsheetFormulaPolicy.Transform(sourceName);

            ValidateFieldCharacterLimit(header.Length, $"CSV export header {index}");
            if (!renderedHeaders.Add(header))
            {
                throw new InvalidDataException(
                    "CSV export rendered header names must be unique ignoring case.");
            }

            bool quoteHeader = NeedsQuoting(header);
            headerSyntaxCharacters = checked(
                headerSyntaxCharacters + GetTextSyntaxCharacters(header, quoteHeader));
            headerByteLength = checked(
                headerByteLength + GetTextUtf8ByteLength(header, quoteHeader));
            if (!string.Equals(header, sourceName, StringComparison.Ordinal))
                transformedHeaderCount++;

            (CsvExportDatabaseType databaseType, CanonicalType canonicalType, string valueEncoding) =
                MapType(column.Type);
            int maximumDecodedBytes =
                databaseType == CsvExportDatabaseType.Blob
                    ? request.MaximumDecodedBlobBytes
                    : 0;

            bindings[index] = new ColumnBinding(
                index,
                sourceName,
                header,
                column.Type,
                canonicalType,
                column.Nullable,
                maximumDecodedBytes,
                quoteHeader);
            manifestColumns[index] = new CsvExportColumnManifest
            {
                Ordinal = index,
                SourceName = sourceName,
                Header = header,
                DatabaseType = databaseType,
                Nullable = column.Nullable,
                ValueEncoding = valueEncoding,
                MaximumDecodedBytes = maximumDecodedBytes,
            };
        }

        ValidateRecordCharacterLimit(headerSyntaxCharacters, "CSV export header");
        if (headerByteLength > request.MaxDataBytes)
        {
            throw new InvalidDataException(
                "CSV export header exceeds the configured data-byte ceiling.");
        }

        CsvExportHashManifest schemaDigest =
            CsvExportManifestSerializer.ComputeSchemaDigest(manifestColumns);
        var table = new CsvExportTableManifest
        {
            Name = tableName,
            SchemaContract = CsvExportContracts.Schema,
            SchemaDigest = schemaDigest,
            RowOrder = CsvExportContracts.RowOrder,
            Columns = Array.AsReadOnly(manifestColumns),
        };
        CsvExportSourceManifest source = CopySource(request.Source);
        CsvExportFormatManifest format = CreateFormat();
        var prepared = new PreparedRequest(
            request.Profile,
            source,
            table,
            format,
            bindings,
            transformedHeaderCount,
            headerByteLength);

        // Validate the complete fixed request/profile/schema/source relationship
        // before a single header byte is written.
        CsvExportManifest provisionalManifest = CreateProvisionalManifest(prepared);
        _ = CsvExportManifestSerializer.Serialize(provisionalManifest);
        return prepared;
    }

    private static PreparedRow PrepareRow(
        CsvExportRow row,
        PreparedRequest request,
        long bytesAlreadyWritten,
        long maximumDataBytes)
    {
        if (row.Values.Length != request.Bindings.Length)
        {
            throw new InvalidDataException(
                $"CSV export row {row.RowId} has {row.Values.Length} fields; " +
                $"{request.Bindings.Length} were required.");
        }

        var fields = new PreparedField[request.Bindings.Length];
        var sourceLogicalValues = new CanonicalValue[request.Bindings.Length];
        var exportedLogicalValues = new CanonicalValue[request.Bindings.Length];
        long recordSyntaxCharacters = request.Bindings.Length - 1L;
        long recordByteLength = request.Bindings.Length - 1L + 2L;
        int transformedCellCount = 0;
        ReadOnlySpan<DbValue> values = row.Values.Span;

        try
        {
            for (int index = 0; index < request.Bindings.Length; index++)
            {
                ColumnBinding binding = request.Bindings[index];
                DbValue value = values[index];
                if (value.IsNull)
                {
                    if (!binding.Nullable)
                    {
                        throw new InvalidDataException(
                            $"CSV export row {row.RowId}, column '{binding.SourceName}' is null " +
                            "but the source schema marks it non-nullable.");
                    }

                    fields[index] = PreparedField.Null;
                    sourceLogicalValues[index] = CanonicalValue.Null(binding.CanonicalType);
                    exportedLogicalValues[index] = CanonicalValue.Null(binding.CanonicalType);
                    recordSyntaxCharacters = checked(
                        recordSyntaxCharacters + CsvExportContracts.NullToken.Length);
                    recordByteLength = checked(
                        recordByteLength + CsvExportContracts.NullToken.Length);
                    continue;
                }

                if (value.Type != binding.SourceType)
                {
                    throw new InvalidDataException(
                        $"CSV export row {row.RowId}, column '{binding.SourceName}' has " +
                        $"runtime type {value.Type}; source type {binding.SourceType} was required.");
                }

                switch (binding.SourceType)
                {
                    case DbType.Integer:
                        {
                            long scalar = value.AsInteger;
                            string lexical = scalar.ToString(CultureInfo.InvariantCulture);
                            fields[index] = PreparedField.Text(lexical, quote: false);
                            CanonicalValue canonical = CanonicalValue.Int64(scalar);
                            sourceLogicalValues[index] = canonical;
                            exportedLogicalValues[index] = canonical;
                            recordSyntaxCharacters = checked(
                                recordSyntaxCharacters + lexical.Length);
                            recordByteLength = checked(recordByteLength + lexical.Length);
                            break;
                        }
                    case DbType.Real:
                        {
                            double scalar = value.AsReal;
                            if (!double.IsFinite(scalar))
                            {
                                throw new InvalidDataException(
                                    $"CSV export row {row.RowId}, column '{binding.SourceName}' " +
                                    "contains a non-finite REAL.");
                            }

                            string lexical = scalar.ToString("R", CultureInfo.InvariantCulture);
                            fields[index] = PreparedField.Text(lexical, quote: false);
                            CanonicalValue canonical = CanonicalValue.Binary64(scalar);
                            sourceLogicalValues[index] = canonical;
                            exportedLogicalValues[index] = canonical;
                            recordSyntaxCharacters = checked(
                                recordSyntaxCharacters + lexical.Length);
                            recordByteLength = checked(recordByteLength + lexical.Length);
                            break;
                        }
                    case DbType.Text:
                        {
                            string sourceText = value.AsText;
                            bool requiresTransform =
                                request.Profile == CsvExportProfile.SpreadsheetSafeLossyV1 &&
                                CsvSpreadsheetFormulaPolicy.RequiresTransform(sourceText);
                            long exportedCharacterLength = checked(
                                sourceText.Length + (requiresTransform ? 1L : 0L));
                            ValidateFieldCharacterLimit(
                                exportedCharacterLength,
                                $"CSV export row {row.RowId}, column '{binding.SourceName}'");
                            ValidateCellText(
                                sourceText,
                                $"CSV export row {row.RowId}, column '{binding.SourceName}'");

                            string exportedText = requiresTransform
                                ? CsvSpreadsheetFormulaPolicy.Transform(sourceText)
                                : sourceText;
                            bool quote = NeedsQuoting(exportedText);
                            fields[index] = PreparedField.Text(exportedText, quote);
                            sourceLogicalValues[index] = CanonicalValue.Text(sourceText);
                            exportedLogicalValues[index] = CanonicalValue.Text(exportedText);
                            if (requiresTransform)
                                transformedCellCount++;
                            recordSyntaxCharacters = checked(
                                recordSyntaxCharacters + GetTextSyntaxCharacters(exportedText, quote));
                            recordByteLength = checked(
                                recordByteLength + GetTextUtf8ByteLength(exportedText, quote));
                            break;
                        }
                    case DbType.Blob:
                        {
                            byte[] sourceBlob = value.AsBlob;
                            if (sourceBlob.Length > binding.MaximumDecodedBytes)
                            {
                                throw new InvalidDataException(
                                    $"CSV export row {row.RowId}, column '{binding.SourceName}' BLOB " +
                                    $"exceeds its {binding.MaximumDecodedBytes}-byte decoded ceiling.");
                            }

                            int encodedLength = checked(((sourceBlob.Length + 2) / 3) * 4);
                            ValidateFieldCharacterLimit(
                                encodedLength,
                                $"CSV export row {row.RowId}, column '{binding.SourceName}'");
                            byte[] blob = sourceBlob.ToArray();
                            fields[index] = PreparedField.Blob(blob);
                            CanonicalValue canonical = CanonicalValue.Blob(blob);
                            sourceLogicalValues[index] = canonical;
                            exportedLogicalValues[index] = canonical;
                            recordSyntaxCharacters = checked(
                                recordSyntaxCharacters + encodedLength);
                            recordByteLength = checked(recordByteLength + encodedLength);
                            break;
                        }
                    default:
                        throw new InvalidOperationException(
                            $"Prepared CSV export type {binding.SourceType} is unsupported.");
                }
            }

            ValidateRecordCharacterLimit(
                recordSyntaxCharacters,
                $"CSV export row {row.RowId}");
            if (recordByteLength > maximumDataBytes - bytesAlreadyWritten)
            {
                throw new InvalidDataException(
                    $"CSV export row {row.RowId} exceeds the configured data-byte ceiling.");
            }

            return new PreparedRow(
                fields,
                sourceLogicalValues,
                exportedLogicalValues,
                transformedCellCount,
                recordByteLength);
        }
        catch
        {
            ClearPreparedBlobs(fields);
            throw;
        }
    }

    private static async ValueTask WriteHeaderAsync(
        ExportByteSink sink,
        PreparedRequest request,
        CancellationToken cancellationToken)
    {
        sink.EnsureCanWrite(request.HeaderByteLength);
        for (int index = 0; index < request.Bindings.Length; index++)
        {
            if (index != 0)
                await sink.WriteAsync(s_comma, cancellationToken).ConfigureAwait(false);
            ColumnBinding binding = request.Bindings[index];
            await WriteTextFieldAsync(
                    sink,
                    binding.Header,
                    binding.QuoteHeader,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        await sink.WriteAsync(s_crlf, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask WriteRowAsync(
        ExportByteSink sink,
        PreparedRow row,
        CancellationToken cancellationToken)
    {
        sink.EnsureCanWrite(row.RecordByteLength);
        for (int index = 0; index < row.Fields.Length; index++)
        {
            if (index != 0)
                await sink.WriteAsync(s_comma, cancellationToken).ConfigureAwait(false);

            PreparedField field = row.Fields[index];
            switch (field.Kind)
            {
                case PreparedFieldKind.Null:
                    await sink.WriteAsync(s_nullToken, cancellationToken).ConfigureAwait(false);
                    break;
                case PreparedFieldKind.Text:
                    await WriteTextFieldAsync(
                            sink,
                            field.TextValue!,
                            field.Quote,
                            cancellationToken)
                        .ConfigureAwait(false);
                    break;
                case PreparedFieldKind.Blob:
                    await WriteBlobAsync(
                            sink,
                            field.BlobValue!,
                            cancellationToken)
                        .ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException("Unknown prepared CSV export field.");
            }
        }
        await sink.WriteAsync(s_crlf, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask WriteTextFieldAsync(
        ExportByteSink sink,
        string value,
        bool quote,
        CancellationToken cancellationToken)
    {
        if (!quote)
        {
            await WriteUtf8Async(sink, value, cancellationToken).ConfigureAwait(false);
            return;
        }

        await sink.WriteAsync(s_quote, cancellationToken).ConfigureAwait(false);
        int offset = 0;
        while (offset < value.Length)
        {
            int quoteIndex = value.IndexOf('"', offset);
            int end = quoteIndex < 0 ? value.Length : quoteIndex;
            if (end != offset)
            {
                await WriteUtf8Async(
                        sink,
                        value.AsMemory(offset, end - offset),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            if (quoteIndex < 0)
                break;

            await sink.WriteAsync(s_escapedQuote, cancellationToken).ConfigureAwait(false);
            offset = quoteIndex + 1;
        }
        await sink.WriteAsync(s_quote, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask WriteUtf8Async(
        ExportByteSink sink,
        string value,
        CancellationToken cancellationToken) =>
        await WriteUtf8Async(sink, value.AsMemory(), cancellationToken).ConfigureAwait(false);

    private static async ValueTask WriteUtf8Async(
        ExportByteSink sink,
        ReadOnlyMemory<char> value,
        CancellationToken cancellationToken)
    {
        if (value.IsEmpty)
            return;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(Utf8BufferBytes);
        try
        {
            int consumedCharacters = 0;
            while (consumedCharacters < value.Length)
            {
                int charactersUsed = Math.Min(
                    Utf8InputChunkCharacters,
                    value.Length - consumedCharacters);
                int boundary = consumedCharacters + charactersUsed;
                if (boundary < value.Length &&
                    char.IsHighSurrogate(value.Span[boundary - 1]))
                {
                    charactersUsed--;
                }
                if (charactersUsed <= 0)
                    throw new InvalidOperationException("Strict UTF-8 chunking made no progress.");

                int bytesUsed = s_strictUtf8.GetBytes(
                    value.Span.Slice(consumedCharacters, charactersUsed),
                    buffer);
                consumedCharacters += charactersUsed;
                await sink.WriteAsync(
                        buffer.AsMemory(0, bytesUsed),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static async ValueTask WriteBlobAsync(
        ExportByteSink sink,
        byte[] blob,
        CancellationToken cancellationToken)
    {
        if (blob.Length == 0)
            return;

        byte[] output = ArrayPool<byte>.Shared.Rent(
            Base64.GetMaxEncodedToUtf8Length(BlobInputChunkBytes));
        try
        {
            int offset = 0;
            while (offset < blob.Length)
            {
                int count = Math.Min(BlobInputChunkBytes, blob.Length - offset);
                bool isFinalBlock = offset + count == blob.Length;
                OperationStatus status = Base64.EncodeToUtf8(
                    blob.AsSpan(offset, count),
                    output,
                    out int bytesConsumed,
                    out int bytesWritten,
                    isFinalBlock);
                if (status != OperationStatus.Done || bytesConsumed != count)
                {
                    throw new InvalidOperationException(
                        "The deterministic padded base64 encoder did not complete.");
                }

                await sink.WriteAsync(
                        output.AsMemory(0, bytesWritten),
                        cancellationToken)
                    .ConfigureAwait(false);
                offset += bytesConsumed;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(output, clearArray: true);
        }
    }

    private static bool NeedsQuoting(string value) =>
        string.Equals(value, CsvExportContracts.NullToken, StringComparison.Ordinal) ||
        value.Contains(',', StringComparison.Ordinal) ||
        value.Contains('"', StringComparison.Ordinal) ||
        value.Contains('\r', StringComparison.Ordinal) ||
        value.Contains('\n', StringComparison.Ordinal);

    private static long GetTextSyntaxCharacters(string value, bool quote)
    {
        if (!quote)
            return value.Length;

        long quoteCount = 0;
        foreach (char character in value)
        {
            if (character == '"')
                quoteCount++;
        }
        return checked(value.Length + quoteCount + 2L);
    }

    private static long GetTextUtf8ByteLength(string value, bool quote)
    {
        long byteLength = s_strictUtf8.GetByteCount(value);
        if (!quote)
            return byteLength;

        long quoteCount = 0;
        foreach (char character in value)
        {
            if (character == '"')
                quoteCount++;
        }
        return checked(byteLength + quoteCount + 2L);
    }

    private static void ValidateManifestText(string value, string description)
    {
        if (value.Contains('\0', StringComparison.Ordinal))
            throw new InvalidDataException($"{description} contains a NUL character.");
        ValidateCellText(value, description);
    }

    private static void ValidateCellText(string value, string description)
    {
        try
        {
            _ = s_strictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidDataException(
                $"{description} contains invalid UTF-16 text.",
                exception);
        }
    }

    private static void ValidateFieldCharacterLimit(long characters, string description)
    {
        if (characters > CsvReaderOptions.MaximumSupportedFieldCharacters)
        {
            throw new InvalidDataException(
                $"{description} exceeds the strict reader's " +
                $"{CsvReaderOptions.MaximumSupportedFieldCharacters}-character field ceiling.");
        }
    }

    private static void ValidateRecordCharacterLimit(long characters, string description)
    {
        if (characters > CsvReaderOptions.MaximumSupportedRecordCharacters)
        {
            throw new InvalidDataException(
                $"{description} exceeds the strict reader's " +
                $"{CsvReaderOptions.MaximumSupportedRecordCharacters}-character record ceiling.");
        }
    }

    private static void ClearPreparedBlobs(IEnumerable<PreparedField> fields)
    {
        foreach (PreparedField field in fields)
        {
            if (field.Kind == PreparedFieldKind.Blob && field.BlobValue is not null)
                CryptographicOperations.ZeroMemory(field.BlobValue);
        }
    }

    private static (
        CsvExportDatabaseType DatabaseType,
        CanonicalType CanonicalType,
        string ValueEncoding)
        MapType(DbType type) => type switch
        {
            DbType.Integer => (
                CsvExportDatabaseType.Integer,
                CanonicalType.Int64,
                CsvExportContracts.IntegerValueEncoding),
            DbType.Real => (
                CsvExportDatabaseType.Real,
                CanonicalType.Binary64,
                CsvExportContracts.RealValueEncoding),
            DbType.Text => (
                CsvExportDatabaseType.Text,
                CanonicalType.Text,
                CsvExportContracts.TextValueEncoding),
            DbType.Blob => (
                CsvExportDatabaseType.Blob,
                CanonicalType.Blob,
                CsvExportContracts.BlobValueEncoding),
            _ => throw new InvalidDataException(
                $"CSV export source type {type} is unsupported."),
        };

    private static CsvExportSourceManifest CopySource(CsvExportSourceManifest source)
    {
        CsvExportHashManifest digest = source.SnapshotDigest is null
            ? null!
            : new CsvExportHashManifest
            {
                Algorithm = source.SnapshotDigest.Algorithm,
                Value = source.SnapshotDigest.Value,
            };
        return new CsvExportSourceManifest
        {
            Kind = source.Kind,
            Version = source.Version,
            SnapshotByteLength = source.SnapshotByteLength,
            SnapshotDigest = digest,
        };
    }

    private static CsvExportFormatManifest CreateFormat() => new()
    {
        Encoding = CsvExportContracts.Encoding,
        HasByteOrderMark = false,
        Culture = CsvExportContracts.Culture,
        Delimiter = ",",
        Quote = '"',
        Newline = CsvExportContracts.Newline,
        HasHeaderRecord = true,
        HasFinalNewline = true,
        NullToken = CsvExportContracts.NullToken,
        NullTokenMatchesQuotedFields = false,
        TextEscape = CsvExportContracts.TextEscape,
    };

    private static CsvExportManifest CreateProvisionalManifest(PreparedRequest request)
    {
        const string zeroHash =
            "0000000000000000000000000000000000000000000000000000000000000000";
        CsvExportHashManifest hash = new()
        {
            Algorithm = CsvExportHashManifest.Sha256Algorithm,
            Value = zeroHash,
        };
        return new CsvExportManifest
        {
            Profile = request.Profile,
            Source = request.Source,
            Table = request.Table,
            Csv = request.Format,
            Content = new CsvExportContentManifest
            {
                RowCount = 0,
                DataByteLength = request.HeaderByteLength,
                DataDigest = hash,
                Canonicalization = CsvExportContracts.Canonicalization,
                CanonicalizationContractDigest =
                    CsvExportContracts.CanonicalizationContractDigest,
                Aggregation = CsvExportContracts.OrderedContentDigest,
                SourceLogicalDigest = hash,
                ExportedLogicalDigest = hash,
            },
            LossyTransform = request.Profile == CsvExportProfile.SpreadsheetSafeLossyV1
                ? new CsvExportLossyTransformManifest
                {
                    RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                    Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                    TransformedHeaderCount = request.TransformedHeaderCount,
                    TransformedRowCount = 0,
                    TransformedCellCount = 0,
                }
                : null,
        };
    }

    private static CsvExportManifest CreateManifest(
        PreparedRequest request,
        long rowCount,
        long dataByteLength,
        CsvExportHashManifest dataDigest,
        CsvExportHashManifest sourceLogicalDigest,
        CsvExportHashManifest exportedLogicalDigest,
        long transformedRows,
        long transformedCells) => new()
        {
            Profile = request.Profile,
            Source = request.Source,
            Table = request.Table,
            Csv = request.Format,
            Content = new CsvExportContentManifest
            {
                RowCount = rowCount,
                DataByteLength = dataByteLength,
                DataDigest = dataDigest,
                Canonicalization = CsvExportContracts.Canonicalization,
                CanonicalizationContractDigest =
                CsvExportContracts.CanonicalizationContractDigest,
                Aggregation = CsvExportContracts.OrderedContentDigest,
                SourceLogicalDigest = sourceLogicalDigest,
                ExportedLogicalDigest = exportedLogicalDigest,
            },
            LossyTransform = request.Profile == CsvExportProfile.SpreadsheetSafeLossyV1
            ? new CsvExportLossyTransformManifest
            {
                RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                TransformedHeaderCount = request.TransformedHeaderCount,
                TransformedRowCount = transformedRows,
                TransformedCellCount = transformedCells,
            }
            : null,
        };

    private sealed record PreparedRequest(
        CsvExportProfile Profile,
        CsvExportSourceManifest Source,
        CsvExportTableManifest Table,
        CsvExportFormatManifest Format,
        ColumnBinding[] Bindings,
        int TransformedHeaderCount,
        long HeaderByteLength);

    private sealed record ColumnBinding(
        int Ordinal,
        string SourceName,
        string Header,
        DbType SourceType,
        CanonicalType CanonicalType,
        bool Nullable,
        int MaximumDecodedBytes,
        bool QuoteHeader);

    private sealed record PreparedRow(
        PreparedField[] Fields,
        CanonicalValue[] SourceLogicalValues,
        CanonicalValue[] ExportedLogicalValues,
        int TransformedCellCount,
        long RecordByteLength)
    {
        public void ClearSensitiveBuffers() => ClearPreparedBlobs(Fields);
    }

    private enum PreparedFieldKind
    {
        Null,
        Text,
        Blob,
    }

    private readonly record struct PreparedField(
        PreparedFieldKind Kind,
        string? TextValue,
        byte[]? BlobValue,
        bool Quote)
    {
        public static PreparedField Null { get; } =
            new(PreparedFieldKind.Null, null, null, false);

        public static PreparedField Text(string value, bool quote) =>
            new(PreparedFieldKind.Text, value, null, quote);

        public static PreparedField Blob(byte[] value) =>
            new(PreparedFieldKind.Blob, null, value, false);
    }

    private sealed class ExportByteSink : IDisposable
    {
        private readonly Stream destination;
        private readonly long maximumBytes;
        private readonly IncrementalHash hash;
        private bool completed;
        private bool disposed;

        public ExportByteSink(Stream destination, long maximumBytes)
            : this(
                destination,
                maximumBytes,
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256),
                bytesWritten: 0)
        {
        }

        public ExportByteSink(
            Stream destination,
            long maximumBytes,
            IncrementalHash hash,
            long bytesWritten)
        {
            ArgumentNullException.ThrowIfNull(destination);
            ArgumentNullException.ThrowIfNull(hash);
            if (maximumBytes <= 0)
                throw new ArgumentOutOfRangeException(nameof(maximumBytes));
            if (bytesWritten < 0 || bytesWritten > maximumBytes)
                throw new ArgumentOutOfRangeException(nameof(bytesWritten));

            this.destination = destination;
            this.maximumBytes = maximumBytes;
            this.hash = hash;
            BytesWritten = bytesWritten;
        }

        public long BytesWritten { get; private set; }

        public void EnsureCanWrite(long byteCount)
        {
            ThrowIfUnavailable();
            if (byteCount < 0 || byteCount > maximumBytes - BytesWritten)
                throw new InvalidDataException("CSV export exceeds its configured data-byte ceiling.");
        }

        public async ValueTask WriteAsync(
            ReadOnlyMemory<byte> bytes,
            CancellationToken cancellationToken)
        {
            EnsureCanWrite(bytes.Length);
            await destination.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            hash.AppendData(bytes.Span);
            BytesWritten = checked(BytesWritten + bytes.Length);
        }

        public CsvExportHashManifest GetCurrentHash()
        {
            ThrowIfUnavailable();
            string value = Convert.ToHexString(hash.GetCurrentHash()).ToLowerInvariant();
            return new CsvExportHashManifest
            {
                Algorithm = CsvExportHashManifest.Sha256Algorithm,
                Value = value,
            };
        }

        public CsvExportHashManifest CompleteHash()
        {
            CsvExportHashManifest result = GetCurrentHash();
            completed = true;
            return result;
        }

        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;
            hash.Dispose();
        }

        private void ThrowIfUnavailable()
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (completed)
                throw new InvalidOperationException("CSV export data checksum is already complete.");
        }
    }
}
