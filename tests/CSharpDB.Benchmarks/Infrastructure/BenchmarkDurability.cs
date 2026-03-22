using CSharpDB.Engine;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class BenchmarkDurability
{
    public const string EnvironmentVariableName = "CSHARPDB_BENCH_DURABILITY";

    public static DurabilityMode CurrentMode => Parse(Environment.GetEnvironmentVariable(EnvironmentVariableName));

    public static DatabaseOptions Apply(DatabaseOptions? options = null)
    {
        return (options ?? new DatabaseOptions())
            .ConfigureStorageEngine(builder => builder.UseDurabilityMode(CurrentMode));
    }

    private static DurabilityMode Parse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return DurabilityMode.Durable;

        if (Enum.TryParse<DurabilityMode>(value, ignoreCase: true, out var mode))
            return mode;

        throw new InvalidOperationException(
            $"Invalid {EnvironmentVariableName} value '{value}'. Expected 'Durable' or 'Buffered'.");
    }
}
