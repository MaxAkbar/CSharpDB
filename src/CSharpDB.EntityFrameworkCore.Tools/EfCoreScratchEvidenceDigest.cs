using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal static class EfCoreScratchEvidenceDigest
{
    private const string HistoryDomain =
        "csharpdb-ef-scratch-history/v1";
    private const string ExecutedSqlDomain =
        "csharpdb-ef-scratch-executed-sql/v1";
    private const string IdempotentSqlDomain =
        "csharpdb-ef-scratch-idempotent-sql/v1";

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static string History(
        IReadOnlyList<string> migrationIds,
        int count)
    {
        ArgumentNullException.ThrowIfNull(migrationIds);
        if (count < 0 || count > migrationIds.Count)
            throw new ArgumentOutOfRangeException(nameof(count));

        using var digest = new Accumulator(HistoryDomain);
        digest.AppendInt32(count);
        for (int ordinal = 0; ordinal < count; ordinal++)
        {
            digest.AppendInt32(ordinal);
            digest.AppendString(migrationIds[ordinal]);
        }
        return digest.Finish();
    }

    internal static Accumulator ExecutedSql() =>
        new(ExecutedSqlDomain);

    internal static string IdempotentSql(string script)
    {
        ArgumentNullException.ThrowIfNull(script);
        using var digest = new Accumulator(IdempotentSqlDomain);
        digest.AppendString(script);
        return digest.Finish();
    }

    internal sealed class Accumulator : IDisposable
    {
        private readonly IncrementalHash _hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private bool _finished;

        internal Accumulator(string domain)
        {
            AppendString(domain);
        }

        internal void AppendInt32(int value)
        {
            Span<byte> bytes = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            AppendBytes(bytes);
        }

        internal void AppendString(string value) =>
            AppendBytes(StrictUtf8.GetBytes(value));

        internal void AppendBytes(ReadOnlySpan<byte> bytes)
        {
            if (_finished)
                throw new InvalidOperationException();
            Span<byte> length = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
            _hash.AppendData(length);
            _hash.AppendData(bytes);
        }

        internal string Finish()
        {
            if (_finished)
                throw new InvalidOperationException();
            _finished = true;
            return Convert.ToHexString(_hash.GetHashAndReset())
                .ToLowerInvariant();
        }

        public void Dispose() => _hash.Dispose();
    }
}
