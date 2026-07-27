using System.Globalization;
using System.Runtime.CompilerServices;

namespace CSharpDB.Migration;

/// <summary>
/// Immutable row stream paired with <see cref="SyntheticMigrationSourceInspector"/>.
/// It deliberately includes nulls, BLOBs, logical text codecs, decimals, and
/// multiple batches so apply/resume behavior can be qualified without external data.
/// </summary>
public sealed class SyntheticMigrationDataSource : IMigrationDataSource
{
    public const string FixtureSnapshotIdentity = "synthetic-snapshot:awkward-v1:rows-v1";

    private readonly IReadOnlyDictionary<string, IReadOnlyList<SyntheticRow>> _rows;
    private bool _disposed;

    public SyntheticMigrationDataSource(MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.Synthetic ||
            !string.Equals(catalog.Source.Identity, SyntheticMigrationSourceInspector.FixtureIdentity, StringComparison.Ordinal))
        {
            throw new ArgumentException("Catalog is not the awkward synthetic fixture.", nameof(catalog));
        }

        Source = catalog.Source;
        _rows = CreateRows();
    }

    public MigrationSourceIdentity Source { get; }

    public string SnapshotIdentity => FixtureSnapshotIdentity;

    public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
        MigrationReadRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (request.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Batch size must be positive.");
        if (request.ColumnObjectIds is null || request.ColumnObjectIds.Count == 0)
            throw new ArgumentException("At least one source column is required.", nameof(request));
        if (request.SnapshotToken is not null &&
            !string.Equals(request.SnapshotToken, SnapshotIdentity, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Synthetic source snapshot token does not match the immutable fixture.");
        }
        if (!_rows.TryGetValue(request.SourceObjectId, out IReadOnlyList<SyntheticRow>? rows))
            throw new InvalidDataException($"Synthetic source object '{request.SourceObjectId}' has no row stream.");

        int offset = ParseCursor(request.ResumeCursor, rows.Count);
        long batchOrdinal = offset / request.BatchSize;
        while (offset < rows.Count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = Math.Min(request.BatchSize, rows.Count - offset);
            var batchRows = new MigrationDataRow[count];
            for (int index = 0; index < count; index++)
            {
                SyntheticRow row = rows[offset + index];
                var values = new MigrationSourceValue[request.ColumnObjectIds.Count];
                for (int columnIndex = 0; columnIndex < request.ColumnObjectIds.Count; columnIndex++)
                {
                    string columnObjectId = request.ColumnObjectIds[columnIndex];
                    if (!row.Values.TryGetValue(columnObjectId, out MigrationSourceValue? value))
                    {
                        throw new InvalidDataException(
                            $"Synthetic row for '{request.SourceObjectId}' has no value for '{columnObjectId}'.");
                    }
                    values[columnIndex] = value;
                }

                batchRows[index] = new MigrationDataRow
                {
                    StableKey = row.StableKey,
                    Values = values,
                };
            }

            int nextOffset = offset + count;
            yield return new MigrationDataBatch
            {
                SourceObjectId = request.SourceObjectId,
                SnapshotIdentity = SnapshotIdentity,
                ColumnObjectIds = request.ColumnObjectIds.ToArray(),
                BatchOrdinal = batchOrdinal,
                StartCursor = offset == 0 ? null : Cursor(offset),
                NextCursor = nextOffset == rows.Count ? null : Cursor(nextOffset),
                Rows = batchRows,
            };

            offset = nextOffset;
            batchOrdinal++;
            await Task.Yield();
        }
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private static IReadOnlyDictionary<string, IReadOnlyList<SyntheticRow>> CreateRows()
    {
        var rows = new Dictionary<string, IReadOnlyList<SyntheticRow>>(StringComparer.Ordinal)
        {
            ["syn:table:customers-upper"] = Enumerable.Range(1, 4)
                .Select(index => new SyntheticRow(
                    index.ToString(CultureInfo.InvariantCulture),
                    new Dictionary<string, MigrationSourceValue>(StringComparer.Ordinal)
                    {
                        ["syn:column:customers-upper:id"] = Signed(index),
                        ["syn:column:customers-upper:name"] = Text($"Customer {index}"),
                        ["syn:column:customers-upper:payload"] = index == 2
                            ? Null()
                            : Binary([0x43, 0x53, 0x44, 0x42, (byte)index]),
                        ["syn:column:customers-upper:enabled"] = Boolean(index % 2 == 1),
                        ["syn:column:customers-upper:external-id"] = GuidValue(
                            Guid.Parse($"00000000-0000-0000-0000-{index:000000000000}")),
                    }))
                .ToArray(),
            ["syn:table:customers-lower"] =
            [
                LowerCustomer("A", "alpha", "ALPHA"),
                LowerCustomer("B", "beta", null),
                LowerCustomer("C", "gamma", "GAMMA"),
            ],
            ["syn:table:reserved"] =
            [
                new SyntheticRow("1", Values(("syn:column:reserved:value", Text("reserved-one")))),
                new SyntheticRow("2", Values(("syn:column:reserved:value", Null()))),
            ],
            ["syn:table:orders"] = Enumerable.Range(1, 12)
                .Select(index => new SyntheticRow(
                    index.ToString(CultureInfo.InvariantCulture),
                    new Dictionary<string, MigrationSourceValue>(StringComparer.Ordinal)
                    {
                        ["syn:column:orders:id"] = Signed(index),
                        ["syn:column:orders:customer-id"] = Signed(((index - 1) % 4) + 1),
                        ["syn:column:orders:amount"] = DecimalValue(index == 12
                            ? "12345678901234567890123456789.123456789"
                            : $"{index * 1000}.{index:000000000}"),
                        ["syn:column:orders:tax"] = DecimalValue($"{index * 7}.25"),
                        ["syn:column:orders:ordered-at"] = DateTimeOffsetValue(
                            new DateTimeOffset(2025, 1, index, 10, 30, 0, TimeSpan.FromHours(-8))),
                        ["syn:column:orders:source-counter"] = Unsigned(
                            index == 12 ? ulong.MaxValue : (ulong)(index * 100)),
                    }))
                .ToArray(),
        };
        return rows;
    }

    private static SyntheticRow LowerCustomer(string key, string upper, string? lower) => new(
        key,
        new Dictionary<string, MigrationSourceValue>(StringComparer.Ordinal)
        {
            ["syn:column:customers-lower:code-upper"] = Text(upper),
            ["syn:column:customers-lower:code-lower"] = lower is null ? Null() : Text(lower),
        });

    private static IReadOnlyDictionary<string, MigrationSourceValue> Values(
        params (string ObjectId, MigrationSourceValue Value)[] values) =>
        values.ToDictionary(item => item.ObjectId, item => item.Value, StringComparer.Ordinal);

    private static MigrationSourceValue Null() => new() { Kind = MigrationSourceValueKind.Null };

    private static MigrationSourceValue Signed(long value) => Scalar(
        MigrationSourceValueKind.SignedInteger,
        value.ToString(CultureInfo.InvariantCulture));

    private static MigrationSourceValue Unsigned(ulong value) => Scalar(
        MigrationSourceValueKind.UnsignedInteger,
        value.ToString(CultureInfo.InvariantCulture));

    private static MigrationSourceValue DecimalValue(string value) =>
        Scalar(MigrationSourceValueKind.Decimal, value);

    private static MigrationSourceValue Text(string value) => Scalar(MigrationSourceValueKind.Text, value);

    private static MigrationSourceValue Boolean(bool value) =>
        Scalar(MigrationSourceValueKind.Boolean, value ? "true" : "false");

    private static MigrationSourceValue GuidValue(Guid value) =>
        Scalar(MigrationSourceValueKind.Guid, value.ToString("D", CultureInfo.InvariantCulture));

    private static MigrationSourceValue DateTimeOffsetValue(DateTimeOffset value) =>
        Scalar(MigrationSourceValueKind.DateTimeOffset, value.ToString("O", CultureInfo.InvariantCulture));

    private static MigrationSourceValue Binary(byte[] value) => new()
    {
        Kind = MigrationSourceValueKind.Binary,
        BinaryValue = value,
    };

    private static MigrationSourceValue Scalar(MigrationSourceValueKind kind, string value) => new()
    {
        Kind = kind,
        CanonicalText = value,
    };

    private static int ParseCursor(string? cursor, int rowCount)
    {
        if (cursor is null)
            return 0;
        if (!cursor.StartsWith("row:", StringComparison.Ordinal) ||
            !int.TryParse(cursor.AsSpan(4), NumberStyles.None, CultureInfo.InvariantCulture, out int offset) ||
            offset < 0 || offset > rowCount)
        {
            throw new InvalidDataException("Synthetic source resume cursor is invalid.");
        }
        return offset;
    }

    private static string Cursor(int offset) => $"row:{offset.ToString(CultureInfo.InvariantCulture)}";

    private sealed record SyntheticRow(
        string StableKey,
        IReadOnlyDictionary<string, MigrationSourceValue> Values);
}
