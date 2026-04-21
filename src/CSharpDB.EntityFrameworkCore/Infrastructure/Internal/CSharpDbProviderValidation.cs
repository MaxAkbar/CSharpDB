using System.Data.Common;
using System.Text.RegularExpressions;
using CSharpDB.Data;

namespace CSharpDB.EntityFrameworkCore.Infrastructure.Internal;

internal static partial class CSharpDbProviderValidation
{
    private const string PrivateMemoryDataSource = ":memory:";

    public static void ValidateConnectionConfiguration(
        string? connectionString,
        DbConnection? connection,
        CSharpDbOptionsExtension? optionsExtension = null)
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

        ResolvedEmbeddedConfiguration desiredConfiguration = CSharpDbEmbeddedConfigurationResolver.Resolve(
            builder,
            optionsExtension?.DirectDatabaseOptions,
            optionsExtension?.HybridDatabaseOptions,
            optionsExtension?.StoragePreset,
            optionsExtension?.EmbeddedOpenMode);

        ValidateEmbeddedTuningSupport(builder, desiredConfiguration);

        if (connection is CSharpDbConnection csharpDbConnection && optionsExtension is not null)
            ValidateExistingConnectionTuning(csharpDbConnection, builder, optionsExtension);
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

    private static void ValidateEmbeddedTuningSupport(
        CSharpDbConnectionStringBuilder builder,
        ResolvedEmbeddedConfiguration configuration)
    {
        if (IsPrivateMemory(builder.DataSource) && configuration.EffectiveHybridDatabaseOptions is not null)
        {
            throw new NotSupportedException(
                "HybridDatabaseOptions and hybrid embedded open modes are only supported for file-backed direct connections.");
        }
    }

    private static void ValidateExistingConnectionTuning(
        CSharpDbConnection connection,
        CSharpDbConnectionStringBuilder builder,
        CSharpDbOptionsExtension optionsExtension)
    {
        bool hasProviderTuning = optionsExtension.DirectDatabaseOptions is not null
            || optionsExtension.HybridDatabaseOptions is not null
            || optionsExtension.StoragePreset is not null
            || optionsExtension.EmbeddedOpenMode is not null;

        if (!hasProviderTuning)
            return;

        ResolvedEmbeddedConfiguration actualConfiguration = CSharpDbEmbeddedConfigurationResolver.Resolve(
            builder,
            connection.DirectDatabaseOptions,
            connection.HybridDatabaseOptions);

        ResolvedEmbeddedConfiguration desiredConfiguration = CSharpDbEmbeddedConfigurationResolver.Resolve(
            builder,
            optionsExtension.DirectDatabaseOptions,
            optionsExtension.HybridDatabaseOptions,
            optionsExtension.StoragePreset,
            optionsExtension.EmbeddedOpenMode);

        if (optionsExtension.DirectDatabaseOptions is not null
            && !ReferenceEquals(connection.DirectDatabaseOptions, optionsExtension.DirectDatabaseOptions))
        {
            throw new InvalidOperationException(
                "UseCSharpDb(existingConnection, ...) cannot apply DirectDatabaseOptions that conflict with the supplied CSharpDbConnection.");
        }

        if (optionsExtension.HybridDatabaseOptions is not null
            && !ReferenceEquals(connection.HybridDatabaseOptions, optionsExtension.HybridDatabaseOptions))
        {
            throw new InvalidOperationException(
                "UseCSharpDb(existingConnection, ...) cannot apply HybridDatabaseOptions that conflict with the supplied CSharpDbConnection.");
        }

        if (optionsExtension.StoragePreset is not null
            && actualConfiguration.EffectiveStoragePreset != desiredConfiguration.EffectiveStoragePreset)
        {
            throw new InvalidOperationException(
                "UseCSharpDb(existingConnection, ...) cannot apply a StoragePreset that conflicts with the supplied CSharpDbConnection.");
        }

        if (optionsExtension.EmbeddedOpenMode is not null
            && actualConfiguration.EffectiveOpenMode != desiredConfiguration.EffectiveOpenMode)
        {
            throw new InvalidOperationException(
                "UseCSharpDb(existingConnection, ...) cannot apply an EmbeddedOpenMode that conflicts with the supplied CSharpDbConnection.");
        }
    }
}
