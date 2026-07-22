using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// A single-use, forward-only CSV logical-record reader. The reader owns no
/// file-size-proportional collection; one decoded logical record is live at a
/// time and is protected by explicit field, record, and column bounds.
/// </summary>
public sealed class CsvStreamingReader : IAsyncDisposable
{
    private const int ParserBufferSize = 64 * 1024;

    private readonly Stream source;
    private readonly CsvReaderSettings settings;
    private readonly BoundedTextReader textReader;
    private readonly CsvParser parser;
    private int enumerationStarted;
    private bool disposed;
    private long logicalRecordNumber;
    private long dataRecordNumber;
    private long lastRawRow;
    private int? fieldCount;

    private CsvStreamingReader(
        Stream source,
        CsvReaderSettings settings,
        BoundedTextReader textReader,
        CsvParser parser,
        string resolvedEncodingName,
        bool hasByteOrderMark)
    {
        this.source = source;
        this.settings = settings;
        this.textReader = textReader;
        this.parser = parser;
        ResolvedEncodingName = resolvedEncodingName;
        HasByteOrderMark = hasByteOrderMark;
        fieldCount = settings.ExpectedFieldCount;
    }

    public CsvHeader? Header { get; private set; }

    /// <summary>
    /// Expected record width. For a headerless input without an explicit
    /// width, this becomes available after the first data record is read.
    /// </summary>
    public int? FieldCount => fieldCount;

    public string Delimiter => settings.Delimiter.ToString();

    public char Quote => settings.Quote;

    public string CultureName => settings.Culture.Name;

    public string ResolvedEncodingName { get; }

    public bool HasByteOrderMark { get; }

    public static async ValueTask<CsvStreamingReader> OpenAsync(
        Stream source,
        CsvReaderOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!source.CanRead)
            throw new ArgumentException("The CSV source stream must be readable.", nameof(source));

        CsvReaderSettings settings = CsvReaderSettings.Create(options ?? new CsvReaderOptions());
        CsvConfiguration configuration = CreateConfiguration(settings);
        BoundedTextReader? boundedReader = null;
        CsvStreamingReader? result = null;
        try
        {
            byte[] prefix = new byte[4];
            int prefixLength = 0;
            while (prefixLength < prefix.Length)
            {
                int read = await source.ReadAsync(
                        prefix.AsMemory(prefixLength, prefix.Length - prefixLength),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                prefixLength += read;
            }

            CsvEncodingResolution resolution = CsvEncodingResolver.Resolve(
                prefix.AsSpan(0, prefixLength),
                settings.Encoding,
                settings.DetectEncodingFromByteOrderMarks);
            byte[] replay = prefix.AsSpan(
                    resolution.PreambleLength,
                    prefixLength - resolution.PreambleLength)
                .ToArray();
            var prefixedStream = new PrefixedReadStream(source, replay);
            var streamReader = new StreamReader(
                prefixedStream,
                resolution.Encoding,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: ParserBufferSize,
                leaveOpen: false);
            boundedReader = new BoundedTextReader(
                streamReader,
                settings.Delimiter,
                settings.Quote,
                settings.MaxFieldCharacters,
                settings.MaxRecordCharacters,
                settings.MaxFieldsPerRecord);

            var parser = new CsvParser(boundedReader, configuration, leaveOpen: true);
            result = new CsvStreamingReader(
                source,
                settings,
                boundedReader,
                parser,
                resolution.Encoding.WebName,
                resolution.PreambleLength > 0);

            if (settings.HasHeaderRecord)
                await result.ReadHeaderAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            if (result is not null)
            {
                await result.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                boundedReader?.Dispose();
                if (!settings.LeaveOpen)
                    await source.DisposeAsync().ConfigureAwait(false);
            }
            throw;
        }
    }

    public async IAsyncEnumerable<CsvLogicalRecord> ReadRecordsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (Interlocked.Exchange(ref enumerationStarted, 1) != 0)
            throw new InvalidOperationException("CSV records can be enumerated only once.");

        while (true)
        {
            ParsedRecord? parsed = await ReadNextAsync(cancellationToken).ConfigureAwait(false);
            if (parsed is null)
                yield break;

            dataRecordNumber++;
            ValidateMaximumFieldCount(parsed);

            if (fieldCount is null)
                fieldCount = parsed.Values.Length;

            if (parsed.Values.Length > fieldCount.Value)
            {
                throw Error(
                    CsvDiagnosticRules.ExtraFields,
                    "The CSV record contains more fields than the established record width.",
                    parsed,
                    fieldCount.Value);
            }

            var fields = new CsvLogicalField[fieldCount.Value];
            for (int index = 0; index < parsed.Values.Length; index++)
            {
                string value = parsed.Values[index];
                if (settings.NullToken is not null &&
                    string.Equals(value, settings.NullToken, StringComparison.Ordinal) &&
                    (!parsed.QuotedFields[index] || settings.NullTokenMatchesQuotedFields))
                {
                    fields[index] = new CsvLogicalField(
                        index,
                        CsvFieldKind.Null,
                        null,
                        parsed.QuotedFields[index]);
                }
                else if (value.Length == 0)
                {
                    fields[index] = new CsvLogicalField(
                        index,
                        CsvFieldKind.Empty,
                        string.Empty,
                        parsed.QuotedFields[index]);
                }
                else
                {
                    fields[index] = new CsvLogicalField(
                        index,
                        CsvFieldKind.Text,
                        value,
                        parsed.QuotedFields[index]);
                }
            }

            for (int index = parsed.Values.Length; index < fields.Length; index++)
                fields[index] = new CsvLogicalField(index, CsvFieldKind.Missing, null, false);

            yield return new CsvLogicalRecord(
                parsed.LogicalRecordNumber,
                dataRecordNumber,
                parsed.StartPhysicalLine,
                parsed.EndPhysicalLine,
                parsed.Values.Length,
                fields);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;

        disposed = true;
        parser.Dispose();
        textReader.Dispose();
        if (!settings.LeaveOpen)
            await source.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private async ValueTask ReadHeaderAsync(CancellationToken cancellationToken)
    {
        ParsedRecord? parsed = await ReadNextAsync(cancellationToken).ConfigureAwait(false);
        if (parsed is null)
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.MissingHeader,
                "The CSV input ended before the required header record."));
        }

        ValidateMaximumFieldCount(parsed);
        if (settings.ExpectedFieldCount is not null &&
            parsed.Values.Length != settings.ExpectedFieldCount.Value)
        {
            throw Error(
                CsvDiagnosticRules.HeaderWidthMismatch,
                "The CSV header width does not match the configured field count.",
                parsed);
        }

        fieldCount = parsed.Values.Length;
        Header = new CsvHeader(
            parsed.LogicalRecordNumber,
            parsed.StartPhysicalLine,
            parsed.EndPhysicalLine,
            parsed.Values,
            parsed.QuotedFields);
    }

    private async ValueTask<ParsedRecord?> ReadNextAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long nextLogicalRecord = logicalRecordNumber + 1;
        long? nextDataRecord = settings.HasHeaderRecord && Header is null
            ? null
            : dataRecordNumber + 1;
        long startPhysicalLine = lastRawRow + 1;
        textReader.SetActiveCancellationToken(cancellationToken);

        try
        {
            bool available = await parser.ReadAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (!available)
                return null;

            string[]? currentRecord = parser.Record;
            if (currentRecord is null)
                throw new CsvMalformedDataException();
            string[] values = currentRecord.ToArray();
            bool[]? quotedFields = textReader.TakeQuotedFieldFlags();
            if (quotedFields is null || quotedFields.Length != values.Length)
                throw new CsvMalformedDataException();
            logicalRecordNumber = nextLogicalRecord;
            long endPhysicalLine = Math.Max(startPhysicalLine, parser.RawRow);
            lastRawRow = endPhysicalLine;
            return new ParsedRecord(
                logicalRecordNumber,
                startPhysicalLine,
                endPhysicalLine,
                values,
                quotedFields);
        }
        catch (CsvReadException)
        {
            throw;
        }
        catch (Exception exception) when (
            cancellationToken.IsCancellationRequested &&
            Contains<OperationCanceledException>(exception))
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception exception) when (Contains<CsvMalformedDataException>(exception))
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.MalformedData,
                "The CSV input is not valid RFC 4180 data.",
                nextLogicalRecord,
                nextDataRecord,
                startPhysicalLine,
                Math.Max(startPhysicalLine, parser.RawRow)));
        }
        catch (Exception exception) when (Contains<CsvRecordLimitExceededException>(exception))
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.RecordLimitExceeded,
                "The CSV logical record exceeds the configured character limit.",
                nextLogicalRecord,
                nextDataRecord,
                startPhysicalLine,
                Math.Max(startPhysicalLine, textReader.CurrentPhysicalLine)));
        }
        catch (Exception exception) when (Contains<DecoderFallbackException>(exception))
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.InvalidEncoding,
                "The CSV input contains bytes that are invalid for the resolved encoding."));
        }
        catch (Exception exception) when (Contains<CsvFieldLimitExceededException>(exception))
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.FieldLimitExceeded,
                "A CSV field exceeds the configured character limit.",
                nextLogicalRecord,
                nextDataRecord,
                startPhysicalLine,
                Math.Max(startPhysicalLine, textReader.CurrentPhysicalLine)));
        }
        catch (Exception exception) when (Contains<CsvFieldCountLimitExceededException>(exception))
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.FieldCountLimitExceeded,
                "The CSV record exceeds the configured field-count limit.",
                nextLogicalRecord,
                nextDataRecord,
                startPhysicalLine,
                Math.Max(startPhysicalLine, textReader.CurrentPhysicalLine),
                settings.MaxFieldsPerRecord));
        }
        catch (Exception exception) when (Contains<MaxFieldSizeException>(exception))
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.RecordLimitExceeded,
                "The CSV logical record exceeds the configured character limit.",
                nextLogicalRecord,
                nextDataRecord,
                startPhysicalLine,
                Math.Max(startPhysicalLine, textReader.CurrentPhysicalLine)));
        }
        catch (CsvHelperException)
        {
            throw new CsvReadException(new CsvReadDiagnostic(
                CsvDiagnosticRules.MalformedData,
                "The CSV input is not valid RFC 4180 data.",
                nextLogicalRecord,
                nextDataRecord,
                startPhysicalLine,
                Math.Max(startPhysicalLine, parser.RawRow)));
        }
        finally
        {
            textReader.SetActiveCancellationToken(default);
        }
    }

    private void ValidateMaximumFieldCount(ParsedRecord record)
    {
        if (record.Values.Length <= settings.MaxFieldsPerRecord)
            return;

        throw Error(
            CsvDiagnosticRules.FieldCountLimitExceeded,
            "The CSV record exceeds the configured field-count limit.",
            record,
            settings.MaxFieldsPerRecord);
    }

    private CsvReadException Error(
        string ruleId,
        string message,
        ParsedRecord record,
        int? columnIndex = null) =>
        new(new CsvReadDiagnostic(
            ruleId,
            message,
            record.LogicalRecordNumber,
            dataRecordNumber == 0 ? null : dataRecordNumber,
            record.StartPhysicalLine,
            record.EndPhysicalLine,
            columnIndex));

    private static bool Contains<TException>(Exception exception)
        where TException : Exception
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is TException)
                return true;
        }

        return false;
    }

    private static CsvConfiguration CreateConfiguration(CsvReaderSettings settings)
    {
        var configuration = new CsvConfiguration(settings.Culture)
        {
            Mode = CsvMode.RFC4180,
            HasHeaderRecord = false,
            Delimiter = settings.Delimiter.ToString(),
            Quote = settings.Quote,
            Escape = settings.Quote,
            IgnoreBlankLines = false,
            TrimOptions = TrimOptions.None,
            DetectColumnCountChanges = false,
            ExceptionMessagesContainRawData = false,
            LineBreakInQuotedFieldIsBadData = false,
            BufferSize = ParserBufferSize,
            // BoundedTextReader enforces the logical-value field limit before
            // CsvHelper sees the record. This raw-span ceiling is only a second
            // defense and matches the larger raw record bound so quotes and
            // escaped quotes do not consume the logical field allowance.
            MaxFieldSize = settings.MaxRecordCharacters,
            BadDataFound = _ => throw new CsvMalformedDataException(),
        };

        try
        {
            configuration.Validate();
        }
        catch (CsvHelper.Configuration.ConfigurationException)
        {
            throw new ArgumentException(
                "The CSV parser configuration is invalid.",
                "options");
        }

        return configuration;
    }

    private sealed record ParsedRecord(
        long LogicalRecordNumber,
        long StartPhysicalLine,
        long EndPhysicalLine,
        string[] Values,
        bool[] QuotedFields);

    private sealed class CsvMalformedDataException : Exception
    {
    }

}
