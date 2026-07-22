using System.Text;

namespace CSharpDB.Migration.Files.Csv;

internal static class CsvEncodingResolver
{
    public static Encoding NormalizeConfiguredEncoding(Encoding configuredEncoding)
    {
        ArgumentNullException.ThrowIfNull(configuredEncoding);
        return configuredEncoding.CodePage switch
        {
            65001 => new UTF8Encoding(false, true),
            1200 => new UnicodeEncoding(false, false, true),
            1201 => new UnicodeEncoding(true, false, true),
            12000 => new UTF32Encoding(false, false, true),
            12001 => new UTF32Encoding(true, false, true),
            _ => throw new ArgumentException(
                "The Phase 4A CSV reader supports canonical UTF-8, UTF-16, and UTF-32 encodings.",
                nameof(configuredEncoding)),
        };
    }

    public static CsvEncodingResolution Resolve(
        ReadOnlySpan<byte> prefix,
        Encoding configuredEncoding,
        bool detectEncodingFromByteOrderMarks)
    {
        ArgumentNullException.ThrowIfNull(configuredEncoding);

        if (detectEncodingFromByteOrderMarks)
        {
            // Test UTF-32 before UTF-16 because the little-endian preambles
            // overlap.
            if (prefix.StartsWith(new byte[] { 0x00, 0x00, 0xFE, 0xFF }))
                return new CsvEncodingResolution(new UTF32Encoding(true, false, true), 4, true);
            if (prefix.StartsWith(new byte[] { 0xFF, 0xFE, 0x00, 0x00 }))
                return new CsvEncodingResolution(new UTF32Encoding(false, false, true), 4, true);
            if (prefix.StartsWith(new byte[] { 0xEF, 0xBB, 0xBF }))
                return new CsvEncodingResolution(new UTF8Encoding(false, true), 3, true);
            if (prefix.StartsWith(new byte[] { 0xFE, 0xFF }))
                return new CsvEncodingResolution(new UnicodeEncoding(true, false, true), 2, true);
            if (prefix.StartsWith(new byte[] { 0xFF, 0xFE }))
                return new CsvEncodingResolution(new UnicodeEncoding(false, false, true), 2, true);
        }

        Encoding resolved = detectEncodingFromByteOrderMarks
            ? (Encoding)configuredEncoding.Clone()
            : CreatePreambleFreeDecoder(configuredEncoding);
        var encoding = (Encoding)resolved.Clone();
        encoding.DecoderFallback = DecoderFallback.ExceptionFallback;
        encoding.EncoderFallback = EncoderFallback.ExceptionFallback;
        return new CsvEncodingResolution(encoding, 0, false);
    }

    private static Encoding CreatePreambleFreeDecoder(Encoding configuredEncoding) =>
        configuredEncoding.CodePage switch
        {
            65001 => new UTF8Encoding(false, true),
            1200 => new UnicodeEncoding(false, false, true),
            1201 => new UnicodeEncoding(true, false, true),
            12000 => new UTF32Encoding(false, false, true),
            12001 => new UTF32Encoding(true, false, true),
            _ => throw new ArgumentException(
                "The configured CSV encoding is not canonical.",
                nameof(configuredEncoding)),
        };
}

internal sealed record CsvEncodingResolution(
    Encoding Encoding,
    int PreambleLength,
    bool HasByteOrderMark);
