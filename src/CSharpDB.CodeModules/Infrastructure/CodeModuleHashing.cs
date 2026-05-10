using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.CodeModules.Infrastructure;

internal static class CodeModuleHashing
{
    public static string ComputeSourceHash(string source)
        => ComputeHash(NormalizeSource(source));

    public static string ComputeModuleSetHash(IEnumerable<CodeModuleDefinition> modules)
    {
        var builder = new StringBuilder();
        foreach (CodeModuleDefinition module in modules.OrderBy(module => module.ModuleId, StringComparer.Ordinal))
        {
            string sourceHash = string.IsNullOrWhiteSpace(module.SourceHash)
                ? ComputeSourceHash(module.Source)
                : module.SourceHash;
            builder
                .Append(module.ModuleId).Append('\n')
                .Append(module.Kind).Append('\n')
                .Append(sourceHash).Append('\n');
        }

        return ComputeHash(builder.ToString());
    }

    public static string NormalizeSource(string source)
        => (source ?? string.Empty)
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n');

    private static string ComputeHash(string value)
    {
        byte[] bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }
}
