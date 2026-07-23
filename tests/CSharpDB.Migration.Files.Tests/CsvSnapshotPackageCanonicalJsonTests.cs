using System.Reflection;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvSnapshotPackageCanonicalJsonTests
{
    private static readonly MethodInfo SerializeMethod = typeof(CsvSnapshotPackage).Assembly
        .GetType(
            "CSharpDB.Migration.Files.Csv.CsvSnapshotPackageCanonicalJson",
            throwOnError: true)!
        .GetMethod(
            "Serialize",
            BindingFlags.Static | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(JsonElement)],
            modifiers: null)!;

    [Fact]
    public void CanonicalStringBytesHaveStableEscapesAndLiteralUnicode()
    {
        string value = string.Concat(Enumerable.Range(0, 32).Select(index => (char)index)) +
            "\"\\café_日本😀";
        JsonElement element = JsonSerializer.SerializeToElement(new OrderedValue(value));
        const string expected =
            "{\"value\":\"" +
            "\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007" +
            "\\b\\t\\n\\u000b\\f\\r\\u000e\\u000f" +
            "\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017" +
            "\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f" +
            "\\\"\\\\café_日本😀\"}";

        byte[] actual = Serialize(element);

        Assert.Equal(Encoding.UTF8.GetBytes(expected), actual);
        Assert.Equal(expected, Encoding.UTF8.GetString(actual));
    }

    [Fact]
    public void CanonicalBytesAreDeterministicAndRoundTripWithoutRuntimeReescaping()
    {
        const string value = "prefix\b\t\n\f\r\u001f\"\\é_日本_😀_suffix";
        JsonElement original = JsonSerializer.SerializeToElement(new OrderedValue(value));

        byte[] first = Serialize(original);
        byte[] repeated = Serialize(original);
        using JsonDocument reparsed = JsonDocument.Parse(first);
        byte[] roundTripped = Serialize(reparsed.RootElement);

        Assert.Equal(first, repeated);
        Assert.Equal(first, roundTripped);
        Assert.Equal(value, reparsed.RootElement.GetProperty("value").GetString());
    }

    [Fact]
    public void CanonicalWriterRejectsAnUnpairedSurrogateEscape()
    {
        using JsonDocument document = JsonDocument.Parse("{\"value\":\"\\ud800\"}");

        TargetInvocationException invocation = Assert.Throws<TargetInvocationException>(
            () => SerializeMethod.Invoke(obj: null, parameters: [document.RootElement]));

        Assert.IsType<InvalidDataException>(invocation.InnerException);
    }

    [Fact]
    public async Task PackageWriterRejectsUnpairedSurrogatesFromPublicRetainedInputs()
    {
        using var temporary = new TemporaryDirectory();
        string packagePath = temporary.PathFor("invalid-surrogate.csdbcsv");
        await using CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("id,name\n1,alpha\n")),
            new CsvSourceSnapshotOptions { WorkspacePath = temporary.Root },
            TestContext.Current.CancellationToken);
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            snapshot,
            new CsvReaderOptions(),
            new CsvInspectionOptions { DelimiterCandidates = [","] },
            TestContext.Current.CancellationToken);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            snapshot,
            inspection,
            cancellationToken: TestContext.Current.CancellationToken);
        CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxDataRecords: 10,
            new CsvSchemaInferenceOptions { TableName = "invalid-\ud800-name" },
            TestContext.Current.CancellationToken);

        Exception? error = await Record.ExceptionAsync(async () =>
            await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                TestContext.Current.CancellationToken));

        Assert.NotNull(error);
        Assert.False(File.Exists(packagePath));
    }

    private static byte[] Serialize(JsonElement element) =>
        Assert.IsType<byte[]>(SerializeMethod.Invoke(obj: null, parameters: [element]));

    private sealed record OrderedValue(
        [property: System.Text.Json.Serialization.JsonPropertyName("value")] string Value);

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-package-canonical-json-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string fileName) => Path.Combine(Root, fileName);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
