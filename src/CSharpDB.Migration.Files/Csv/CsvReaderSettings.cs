using System.Globalization;
using System.Text;

namespace CSharpDB.Migration.Files.Csv;

internal sealed record CsvReaderSettings(
    bool HasHeaderRecord,
    char Delimiter,
    char Quote,
    CultureInfo Culture,
    Encoding Encoding,
    bool DetectEncodingFromByteOrderMarks,
    string? NullToken,
    bool NullTokenMatchesQuotedFields,
    int? ExpectedFieldCount,
    int MaxFieldCharacters,
    int MaxRecordCharacters,
    int MaxFieldsPerRecord,
    bool LeaveOpen)
{
    public static CsvReaderSettings Create(CsvReaderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Culture);
        ArgumentNullException.ThrowIfNull(options.Encoding);

        if (options.Delimiter is null || options.Delimiter.Length != 1)
        {
            throw new ArgumentException(
                "The Phase 4A strict reader requires exactly one delimiter character.",
                nameof(options));
        }

        char delimiter = options.Delimiter[0];
        if (delimiter is '\r' or '\n' or '\0' || delimiter == options.Quote)
            throw new ArgumentException("The CSV delimiter is not valid.", nameof(options));
        if (options.Quote is '\r' or '\n' or '\0')
            throw new ArgumentException("The CSV quote character is not valid.", nameof(options));
        if (options.NullToken is not null && options.NullToken.Length == 0)
        {
            throw new ArgumentException(
                "The null token cannot be empty because empty and null must remain distinct.",
                nameof(options));
        }
        if (options.NullToken is not null &&
            !options.NullTokenMatchesQuotedFields &&
            options.NullToken.IndexOfAny([delimiter, options.Quote, '\r', '\n']) >= 0)
        {
            throw new ArgumentException(
                "A reversible unquoted null token cannot contain CSV structural characters.",
                nameof(options));
        }

        if (options.MaxFieldCharacters <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "The field limit must be positive.");
        if (options.MaxRecordCharacters <= 0 ||
            options.MaxRecordCharacters < options.MaxFieldCharacters)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The record limit must be positive and at least the field limit.");
        }

        if (options.MaxFieldsPerRecord <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "The field-count limit must be positive.");
        if (options.ExpectedFieldCount is <= 0 ||
            options.ExpectedFieldCount > options.MaxFieldsPerRecord)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The expected field count must fit within the field-count limit.");
        }

        var culture = CultureInfo.ReadOnly((CultureInfo)options.Culture.Clone());
        Encoding normalizedEncoding = CsvEncodingResolver.NormalizeConfiguredEncoding(options.Encoding);
        var encoding = (Encoding)normalizedEncoding.Clone();
        encoding.DecoderFallback = DecoderFallback.ExceptionFallback;
        encoding.EncoderFallback = EncoderFallback.ExceptionFallback;

        return new CsvReaderSettings(
            options.HasHeaderRecord,
            delimiter,
            options.Quote,
            culture,
            encoding,
            options.DetectEncodingFromByteOrderMarks,
            options.NullToken,
            options.NullTokenMatchesQuotedFields,
            options.ExpectedFieldCount,
            options.MaxFieldCharacters,
            options.MaxRecordCharacters,
            options.MaxFieldsPerRecord,
            options.LeaveOpen);
    }

    public CsvReaderOptions ToOptions(
        char? delimiter = null,
        Encoding? encoding = null,
        bool? detectEncodingFromByteOrderMarks = null,
        bool? hasHeaderRecord = null,
        bool? leaveOpen = null) =>
        new()
        {
            HasHeaderRecord = hasHeaderRecord ?? HasHeaderRecord,
            Delimiter = (delimiter ?? Delimiter).ToString(),
            Quote = Quote,
            Culture = Culture,
            Encoding = encoding ?? Encoding,
            DetectEncodingFromByteOrderMarks =
                detectEncodingFromByteOrderMarks ?? DetectEncodingFromByteOrderMarks,
            NullToken = NullToken,
            NullTokenMatchesQuotedFields = NullTokenMatchesQuotedFields,
            ExpectedFieldCount = ExpectedFieldCount,
            MaxFieldCharacters = MaxFieldCharacters,
            MaxRecordCharacters = MaxRecordCharacters,
            MaxFieldsPerRecord = MaxFieldsPerRecord,
            LeaveOpen = leaveOpen ?? LeaveOpen,
        };
}
