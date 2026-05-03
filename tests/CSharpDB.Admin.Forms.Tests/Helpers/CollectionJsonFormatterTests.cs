using System.Text.Json;
using CSharpDB.Admin.Helpers;

namespace CSharpDB.Admin.Forms.Tests.Helpers;

public sealed class CollectionJsonFormatterTests
{
    [Theory]
    [InlineData("""{"name":"Ada","active":true}""", "object")]
    [InlineData("""["alpha","beta"]""", "array")]
    [InlineData("42", "number")]
    [InlineData("null", "null")]
    public void Format_PreservesSupportedJsonKinds(string json, string expectedKind)
    {
        using var document = JsonDocument.Parse(json);

        string formatted = CollectionJsonFormatter.Format(document.RootElement);

        using var formattedDocument = JsonDocument.Parse(formatted);
        Assert.Equal(expectedKind, CollectionJsonFormatter.GetKindLabel(formattedDocument.RootElement));
    }

    [Fact]
    public void TryClone_RejectsInvalidJson()
    {
        bool result = CollectionJsonFormatter.TryClone("{ invalid", out _, out string? error);

        Assert.False(result);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void GetPreview_ProducesStableObjectPreview()
    {
        using var document = JsonDocument.Parse("""
            {
              "name": "Ada",
              "active": true,
              "score": 42,
              "tags": ["admin"]
            }
            """);

        string preview = CollectionJsonFormatter.GetPreview(document.RootElement);

        Assert.Equal("name: Ada, active: true, score: 42, tags: [...]", preview);
    }

    [Fact]
    public void GetPreview_TruncatesLongPreview()
    {
        using var document = JsonDocument.Parse("""{"name":"abcdefghijklmnopqrstuvwxyz"}""");

        string preview = CollectionJsonFormatter.GetPreview(document.RootElement, maxLength: 12);

        Assert.Equal("name: abc...", preview);
    }
}
