using System.Data.Common;
using System.Text.RegularExpressions;
using CSharpDB.Data;

namespace CSharpDB.EntityFrameworkCore.Infrastructure.Internal;

internal static partial class CSharpDbProviderValidation
{
    private const string PrivateMemoryDataSource = ":memory:";

    public static void ValidateConnectionConfiguration(string? connectionString, DbConnection? connection)
    {
        if (connection is not null && connection is not CSharpDbConnection)
            throw new InvalidOperationException(
                $"UseCSharpDb requires {nameof(CSharpDbConnection)} when an existing connection is supplied.");

        string effectiveConnectionString = connection?.ConnectionString ?? connectionString ?? string.Empty;
        if (string.IsNullOrWhiteSpace(effectiveConnectionString))
            return;

        var builder = new CSharpDbConnectionStringBuilder(effectiveConnectionString);

        if (!string.IsNullOrWhiteSpace(builder.Endpoint))
            throw new NotSupportedException("The EF Core provider only supports embedded databases in v1. Endpoint connections are not supported.");

        if (!string.IsNullOrWhiteSpace(builder.Transport)
            && !string.Equals(builder.Transport, "Direct", StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException("The EF Core provider only supports direct embedded transports in v1.");
        }

        if (builder.Pooling)
            throw new NotSupportedException("The EF Core provider does not support pooled connections in v1.");

        if (IsNamedSharedMemory(builder.DataSource))
        {
            throw new NotSupportedException(
                "The EF Core provider does not support named shared-memory databases in v1. Use a file-backed database or private :memory:.");
        }
    }

    public static bool IsPrivateMemory(string? dataSource)
        => string.Equals(dataSource?.Trim(), PrivateMemoryDataSource, StringComparison.OrdinalIgnoreCase);

    public static bool IsNamedSharedMemory(string? dataSource)
        => !string.IsNullOrWhiteSpace(dataSource)
            && dataSource.StartsWith(PrivateMemoryDataSource, StringComparison.OrdinalIgnoreCase)
            && !IsPrivateMemory(dataSource);

    public static void ValidateSimpleIdentifier(string identifier, string description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(identifier);

        if (!SimpleIdentifierRegex().IsMatch(identifier))
        {
            throw new NotSupportedException(
                $"{description} '{identifier}' requires quoted identifier support, which the CSharpDB EF Core provider does not support in v1.");
        }
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex SimpleIdentifierRegex();
}
