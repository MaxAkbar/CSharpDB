using System.Globalization;
using System.Text;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Controls strict, forward-only CSV parsing. Character limits count decoded
/// UTF-16 code units and apply to one logical record at a time.
/// </summary>
public sealed record CsvReaderOptions
{
    public bool HasHeaderRecord { get; init; } = true;

    public string Delimiter { get; init; } = ",";

    public char Quote { get; init; } = '"';

    public CultureInfo Culture { get; init; } = CultureInfo.InvariantCulture;

    public Encoding Encoding { get; init; } = new UTF8Encoding(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public bool DetectEncodingFromByteOrderMarks { get; init; } = true;

    /// <summary>
    /// An exact decoded value that represents null. A configured token applies
    /// to unquoted fields by default so a quoted token can remain literal text.
    /// </summary>
    public string? NullToken { get; init; }

    /// <summary>
    /// Also treats a quoted field equal to <see cref="NullToken"/> as null.
    /// Enabling this makes that literal text indistinguishable from null and is
    /// therefore unsuitable for a lossless round-trip convention.
    /// </summary>
    public bool NullTokenMatchesQuotedFields { get; init; }

    /// <summary>
    /// Establishes the expected width for a headerless input. When omitted,
    /// the first data record establishes the width.
    /// </summary>
    public int? ExpectedFieldCount { get; init; }

    /// <summary>Maximum decoded logical value length for one field.</summary>
    public int MaxFieldCharacters { get; init; } = 16 * 1024 * 1024;

    /// <summary>
    /// Maximum decoded CSV syntax length for one logical record, including
    /// delimiters and quote syntax but excluding the terminating line break.
    /// </summary>
    public int MaxRecordCharacters { get; init; } = 64 * 1024 * 1024;

    /// <summary>Maximum number of fields before parser materialization.</summary>
    public int MaxFieldsPerRecord { get; init; } = 16_384;

    public bool LeaveOpen { get; init; }
}
