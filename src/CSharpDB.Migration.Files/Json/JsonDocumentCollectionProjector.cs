using System.Text;
using CSharpDB.Migration;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Fully scans an immutable JSON snapshot and freezes the metadata required
/// for the explicit document-collection projection.
/// </summary>
public static class JsonDocumentCollectionProjector
{
    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public static async ValueTask<
        JsonDocumentCollectionProjectionResult> ProjectAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonDocumentCollectionProjectionOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(snapshot);
        JsonDocumentCollectionProjectionOptions settings =
            ValidateAndFreeze(
                options ??
                new JsonDocumentCollectionProjectionOptions());

        cancellationToken.ThrowIfCancellationRequested();
        await using JsonStreamingReader reader = await binding
            .OpenReaderAsync(snapshot, cancellationToken)
            .ConfigureAwait(false);

        long totalRecords = 0;
        long nullRecords = 0;
        long booleanRecords = 0;
        long stringRecords = 0;
        long numberRecords = 0;
        long objectRecords = 0;
        long arrayRecords = 0;
        long maxCanonicalDocumentBytes = 0;

        await foreach (JsonLogicalRecord record in reader
                           .ReadValuesAsync(cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long expectedOrdinal = checked(totalRecords + 1);
            if (record.RecordOrdinal != expectedOrdinal)
            {
                throw new InvalidDataException(
                    "JSON collection record ordinals are not contiguous.");
            }

            totalRecords = expectedOrdinal;
            switch (record.Value.Kind)
            {
                case JsonLogicalValueKind.Null:
                    nullRecords = checked(nullRecords + 1);
                    break;
                case JsonLogicalValueKind.Boolean:
                    booleanRecords = checked(booleanRecords + 1);
                    break;
                case JsonLogicalValueKind.String:
                    stringRecords = checked(stringRecords + 1);
                    break;
                case JsonLogicalValueKind.Number:
                    numberRecords = checked(numberRecords + 1);
                    break;
                case JsonLogicalValueKind.Object:
                    objectRecords = checked(objectRecords + 1);
                    break;
                case JsonLogicalValueKind.Array:
                    arrayRecords = checked(arrayRecords + 1);
                    break;
                default:
                    throw new InvalidDataException(
                        "The JSON collection value kind is unsupported.");
            }

            long canonicalBytes =
                JsonCanonicalValueSerializer
                    .SerializeToUtf8Bytes(
                        record.Value,
                        cancellationToken)
                    .LongLength;
            maxCanonicalDocumentBytes = Math.Max(
                maxCanonicalDocumentBytes,
                canonicalBytes);
        }

        if (checked(
                nullRecords +
                booleanRecords +
                stringRecords +
                numberRecords +
                objectRecords +
                arrayRecords) != totalRecords)
        {
            throw new InvalidDataException(
                "JSON collection value-kind counts are inconsistent.");
        }

        return new JsonDocumentCollectionProjectionResult(
            binding,
            settings.CollectionName,
            totalRecords,
            nullRecords,
            booleanRecords,
            stringRecords,
            numberRecords,
            objectRecords,
            arrayRecords,
            maxCanonicalDocumentBytes);
    }

    private static JsonDocumentCollectionProjectionOptions
        ValidateAndFreeze(
            JsonDocumentCollectionProjectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.CollectionName) ||
            options.CollectionName.Length >
                JsonDocumentCollectionProjectionOptions
                    .MaximumCollectionNameCharacters)
        {
            throw new ArgumentException(
                $"The JSON collection name must be nonblank and at most {JsonDocumentCollectionProjectionOptions.MaximumCollectionNameCharacters} characters.",
                nameof(options));
        }

        try
        {
            SqlIdentifierRules.Validate(
                options.CollectionName,
                "JSON collection name");
            _ = s_strictUtf8.GetByteCount(
                options.CollectionName);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException(
                "The JSON collection name contains invalid Unicode.",
                nameof(options),
                exception);
        }

        return new JsonDocumentCollectionProjectionOptions
        {
            CollectionName = options.CollectionName,
        };
    }
}
