using System.Reflection;
using CSharpDB.Primitives;
using CSharpDB.Execution;

namespace CSharpDB.Tests;

public sealed class SqlIndexCompatibilityTests
{
    private delegate long ComputeIndexKeyDelegate(ReadOnlySpan<DbValue> keyComponents);

    private static readonly ComputeIndexKeyDelegate ComputeIndexKey = CreateComputeIndexKeyDelegate();

    [Fact]
    public void ComputeIndexKey_PreservesLegacySqlTextHash()
    {
        DbValue[][] testCases =
        {
            new[] { DbValue.FromText("Alice") },
            new[] { DbValue.FromText("cafe") },
            new[] { DbValue.FromText("cafe\u0301") },
            new[] { DbValue.FromInteger(42), DbValue.FromText("delta") },
        };

        foreach (DbValue[] keyComponents in testCases)
        {
            long actual = ComputeIndexKey(keyComponents);
            long expected = ComputeLegacySqlIndexKey(keyComponents);
            Assert.Equal(expected, actual);
        }
    }

    private static ComputeIndexKeyDelegate CreateComputeIndexKeyDelegate()
    {
        Type helperType = typeof(QueryPlanner).Assembly.GetType(
            "CSharpDB.Execution.IndexMaintenanceHelper",
            throwOnError: true)!;
        MethodInfo method = helperType.GetMethod(
            "ComputeIndexKey",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)!;
        return (ComputeIndexKeyDelegate)method.CreateDelegate(typeof(ComputeIndexKeyDelegate));
    }

    private static long ComputeLegacySqlIndexKey(ReadOnlySpan<DbValue> keyComponents)
    {
        if (keyComponents.Length == 1 && keyComponents[0].Type == DbType.Integer)
            return keyComponents[0].AsInteger;

        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int i = 0; i < keyComponents.Length; i++)
            hash = HashLegacySqlIndexKeyComponent(hash, keyComponents[i], prime);

        return unchecked((long)hash);
    }

    private static ulong HashLegacySqlIndexKeyComponent(ulong hash, DbValue value, ulong prime)
    {
        hash ^= (byte)value.Type;
        hash *= prime;

        switch (value.Type)
        {
            case DbType.Integer:
                hash ^= unchecked((ulong)value.AsInteger);
                hash *= prime;
                return hash;
            case DbType.Text:
            {
                string text = value.AsText;
                for (int i = 0; i < text.Length; i++)
                {
                    char c = text[i];
                    hash ^= (byte)c;
                    hash *= prime;
                    hash ^= (byte)(c >> 8);
                    hash *= prime;
                }

                return hash;
            }
            default:
                throw new InvalidOperationException($"Unsupported indexed value type '{value.Type}'.");
        }
    }
}
