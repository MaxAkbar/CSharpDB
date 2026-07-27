namespace CSharpDB.Migration.Files.Csv;

internal sealed class CsvRecordLimitExceededException(long limit)
    : Exception("The decoded CSV syntax exceeded its configured record limit.")
{
    public long Limit { get; } = limit;
}

internal sealed class CsvFieldLimitExceededException(long limit)
    : Exception("The decoded CSV field exceeded its configured character limit.")
{
    public long Limit { get; } = limit;
}

internal sealed class CsvFieldCountLimitExceededException(int limit)
    : Exception("The CSV record exceeded its configured field-count limit.")
{
    public int Limit { get; } = limit;
}

/// <summary>
/// Enforces a logical-record bound before CsvHelper can grow its record buffer.
/// It also enforces decoded logical-field and field-count limits before parser
/// materialization.
/// </summary>
internal sealed class BoundedTextReader : TextReader
{
    private readonly TextReader inner;
    private readonly char delimiter;
    private readonly char quote;
    private readonly long maximumFieldCharacters;
    private readonly long maximumRecordCharacters;
    private readonly int maximumFieldsPerRecord;
    private readonly Queue<bool[]> completedQuotedFields = new();
    private readonly List<bool> currentQuotedFields = [];
    private long fieldCharacters;
    private long recordCharacters;
    private int currentFieldCount = 1;
    private bool atFieldStart = true;
    private bool inQuotedField;
    private bool quotePending;
    private bool currentFieldWasQuoted;
    private bool recordHasCharacters;
    private bool endOfInputObserved;
    private bool failed;
    private bool previousBoundaryWasCarriageReturn;
    private bool previousPhysicalCharacterWasCarriageReturn;
    private char[]? pendingBuffer;
    private int pendingOffset;
    private CancellationToken activeCancellationToken;

    public long CurrentPhysicalLine { get; private set; } = 1;

    public BoundedTextReader(
        TextReader inner,
        char delimiter,
        char quote,
        long maximumFieldCharacters,
        long maximumRecordCharacters,
        int maximumFieldsPerRecord)
    {
        this.inner = inner;
        this.delimiter = delimiter;
        this.quote = quote;
        this.maximumFieldCharacters = maximumFieldCharacters;
        this.maximumRecordCharacters = maximumRecordCharacters;
        this.maximumFieldsPerRecord = maximumFieldsPerRecord;
    }

    public void SetActiveCancellationToken(CancellationToken cancellationToken) =>
        activeCancellationToken = cancellationToken;

    public bool[]? TakeQuotedFieldFlags() =>
        completedQuotedFields.Count == 0 ? null : completedQuotedFields.Dequeue();

    public override int Peek() => pendingBuffer is not null
        ? pendingBuffer[pendingOffset]
        : inner.Peek();

    public override int Read()
    {
        int value;
        if (pendingBuffer is not null)
        {
            value = pendingBuffer[pendingOffset++];
            ClearPendingIfConsumed();
        }
        else
        {
            value = inner.Read();
        }

        if (value >= 0)
        {
            if (previousBoundaryWasCarriageReturn)
            {
                previousBoundaryWasCarriageReturn = false;
                if (value == '\n')
                {
                    previousPhysicalCharacterWasCarriageReturn = false;
                    return value;
                }
            }

            char character = (char)value;
            Observe(character);
            AdvancePhysicalLine(character);
        }
        else
        {
            ObserveEndOfInput();
        }
        return value;
    }

    public override int Read(char[] buffer, int index, int count)
    {
        int pending = DrainPending(buffer.AsSpan(index, count));
        if (pending > 0 || count == 0)
            return pending;

        int read = inner.Read(buffer, index, count);
        if (read == 0)
            ObserveEndOfInput();
        return ObserveAndBufferRemainder(buffer.AsSpan(index, read));
    }

    public override int Read(Span<char> buffer)
    {
        int pending = DrainPending(buffer);
        if (pending > 0 || buffer.IsEmpty)
            return pending;

        int read = inner.Read(buffer);
        if (read == 0)
            ObserveEndOfInput();
        return ObserveAndBufferRemainder(buffer[..read]);
    }

    public override async Task<int> ReadAsync(char[] buffer, int index, int count)
    {
        activeCancellationToken.ThrowIfCancellationRequested();
        int pending = DrainPending(buffer.AsSpan(index, count));
        if (pending > 0 || count == 0)
            return pending;

        int read = await inner.ReadAsync(
                buffer.AsMemory(index, count),
                activeCancellationToken)
            .ConfigureAwait(false);
        if (read == 0)
            ObserveEndOfInput();
        return ObserveAndBufferRemainder(buffer.AsSpan(index, read));
    }

    public override async ValueTask<int> ReadAsync(
        Memory<char> buffer,
        CancellationToken cancellationToken = default)
    {
        CancellationToken effectiveCancellationToken = activeCancellationToken.CanBeCanceled
            ? activeCancellationToken
            : cancellationToken;
        effectiveCancellationToken.ThrowIfCancellationRequested();
        int pending = DrainPending(buffer.Span);
        if (pending > 0 || buffer.IsEmpty)
            return pending;

        int read = await inner.ReadAsync(buffer, effectiveCancellationToken).ConfigureAwait(false);
        if (read == 0)
            ObserveEndOfInput();
        return ObserveAndBufferRemainder(buffer.Span[..read]);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();
        completedQuotedFields.Clear();
        currentQuotedFields.Clear();
        base.Dispose(disposing);
    }

    private int DrainPending(Span<char> destination)
    {
        if (pendingBuffer is null || destination.IsEmpty)
            return 0;

        int candidateCount = Math.Min(destination.Length, pendingBuffer.Length - pendingOffset);
        pendingBuffer.AsSpan(pendingOffset, candidateCount).CopyTo(destination);
        int acceptedCount = ObserveUntilBoundary(destination[..candidateCount]);
        pendingOffset += acceptedCount;
        ClearPendingIfConsumed();
        return acceptedCount;
    }

    private int ObserveAndBufferRemainder(ReadOnlySpan<char> value)
    {
        int acceptedCount = ObserveUntilBoundary(value);
        if (acceptedCount < value.Length)
        {
            pendingBuffer = value[acceptedCount..].ToArray();
            pendingOffset = 0;
        }

        return acceptedCount;
    }

    private int ObserveUntilBoundary(ReadOnlySpan<char> value)
    {
        if (previousBoundaryWasCarriageReturn && !value.IsEmpty)
        {
            previousBoundaryWasCarriageReturn = false;
            if (value[0] == '\n')
            {
                previousPhysicalCharacterWasCarriageReturn = false;
                return 1;
            }
        }

        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];
            bool isBoundary = Observe(character);
            AdvancePhysicalLine(character);
            if (!isBoundary)
                continue;

            if (character == '\r')
            {
                if (index + 1 < value.Length && value[index + 1] == '\n')
                {
                    previousPhysicalCharacterWasCarriageReturn = false;
                    return index + 2;
                }
                previousBoundaryWasCarriageReturn = true;
            }

            return index + 1;
        }

        return value.Length;
    }

    private bool Observe(char character)
    {
        if (failed)
            throw new CsvRecordLimitExceededException(maximumRecordCharacters);

        if (inQuotedField)
        {
            if (quotePending)
            {
                if (character == quote)
                {
                    quotePending = false;
                    Count(character);
                    CountFieldCharacter();
                    return false;
                }

                inQuotedField = false;
                quotePending = false;
                return ObserveOutsideQuotedField(character);
            }

            Count(character);
            if (character == quote)
                quotePending = true;
            else
                CountFieldCharacter();
            return false;
        }

        return ObserveOutsideQuotedField(character);
    }

    private bool ObserveOutsideQuotedField(char character)
    {
        if (character is '\r' or '\n')
        {
            CompleteRecord();
            recordCharacters = 0;
            fieldCharacters = 0;
            atFieldStart = true;
            return true;
        }

        Count(character);

        if (atFieldStart && character == quote)
        {
            inQuotedField = true;
            currentFieldWasQuoted = true;
            atFieldStart = false;
        }
        else
        {
            if (character == delimiter)
            {
                CompleteField();
                fieldCharacters = 0;
                atFieldStart = true;
            }
            else
            {
                CountFieldCharacter();
                atFieldStart = false;
            }
        }

        return false;
    }

    private void Count(char _)
    {
        recordHasCharacters = true;
        recordCharacters++;
        if (recordCharacters <= maximumRecordCharacters)
            return;

        failed = true;
        throw new CsvRecordLimitExceededException(maximumRecordCharacters);
    }

    private void CountFieldCharacter()
    {
        fieldCharacters++;
        if (fieldCharacters <= maximumFieldCharacters)
            return;

        failed = true;
        throw new CsvFieldLimitExceededException(maximumFieldCharacters);
    }

    private void CompleteField()
    {
        currentQuotedFields.Add(currentFieldWasQuoted);
        currentFieldWasQuoted = false;
        currentFieldCount++;
        if (currentFieldCount <= maximumFieldsPerRecord)
            return;

        failed = true;
        throw new CsvFieldCountLimitExceededException(maximumFieldsPerRecord);
    }

    private void CompleteRecord()
    {
        currentQuotedFields.Add(currentFieldWasQuoted);
        completedQuotedFields.Enqueue(currentQuotedFields.ToArray());
        currentQuotedFields.Clear();
        currentFieldWasQuoted = false;
        currentFieldCount = 1;
        recordHasCharacters = false;
        inQuotedField = false;
        quotePending = false;
    }

    private void ObserveEndOfInput()
    {
        if (endOfInputObserved)
            return;

        endOfInputObserved = true;
        if (recordHasCharacters)
            CompleteRecord();
    }

    private void AdvancePhysicalLine(char character)
    {
        if (character == '\r')
        {
            CurrentPhysicalLine++;
            previousPhysicalCharacterWasCarriageReturn = true;
            return;
        }

        if (character == '\n')
        {
            if (!previousPhysicalCharacterWasCarriageReturn)
                CurrentPhysicalLine++;
            previousPhysicalCharacterWasCarriageReturn = false;
            return;
        }

        previousPhysicalCharacterWasCarriageReturn = false;
    }

    private void ClearPendingIfConsumed()
    {
        if (pendingBuffer is null || pendingOffset < pendingBuffer.Length)
            return;

        pendingBuffer = null;
        pendingOffset = 0;
    }
}
